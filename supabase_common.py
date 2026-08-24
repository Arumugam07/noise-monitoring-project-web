"""
Shared helpers for the Supabase ETL scripts.

Who should read this:
- Beginners who want a clear, simple place to see the core logic shared
  by both daily and backfill-all jobs without duplicating code.

What this file contains:
- Timezone setup for Singapore Time ("SGT") and API default URL.
- The list of device locations (`LOCATIONS`) we pull data for.
- Utility functions:
  - `build_rows(...)`: turns raw API data for one device-day into
    per-minute rows with correct UTC timestamps.
  - `upsert_rows(...)`: safely writes rows into Supabase in small chunks
    and avoids duplicates using the composite unique key
    `(location_id, reading_datetime)`.
  - `yesterday_sgt()`: convenience function to calculate yesterday's
    date in Singapore local time.

Why have a common file at all?
- It keeps the two job scripts (daily and backfill-all) short and easy
  to read, avoiding copy-pasting the same logic into multiple places.
  If you prefer everything in one file, we can inline these helpers,
  but this layout is generally easier to maintain.

CHANGELOG (build_rows):
- Fixed a day-boundary bug: the upstream API interprets naive start/end
  query params AS UTC, not SGT. The old code sent "{day}T00:00:00" /
  "{day}T23:59:59" naively, intending an SGT calendar day, but actually
  got the UTC calendar day instead (08:00 SGT that day -> ~07:59 SGT
  the next day). Fixed by converting SGT day boundaries to UTC before
  building the query string.
- Added retry (up to 2 retries, 2s backoff) for transient failures
  (timeouts, connection errors, 5xx), and treats a real 404 as
  "confirmed no data" without retrying it.
"""

import logging
import os
import time
from datetime import datetime, timedelta, date, timezone
from typing import List, Dict

import requests
from supabase import Client

try:
    import zoneinfo  # py3.9+
except ImportError:
    from backports import zoneinfo

SGT = zoneinfo.ZoneInfo("Asia/Singapore")
API_DEFAULT = "http://139.59.223.231:3000/api/meter-sound"

# 13 device locations
LOCATIONS: List[Dict[str, str]] = [
    {"ID":"15490","Name":"Singapore Sports School"},
    {"ID":"16034","Name":"BLK 120 Serangoon North Ave 1"},
    {"ID":"16041","Name":"BLK 838 Hougang Central"},
    {"ID":"14542","Name":"BLK 558 Jurong West Street 42"},
    {"ID":"15725","Name":"Jurong Safra, Block C"},
    {"ID":"16032","Name":"AMA KENG SITE"},
    {"ID":"16045","Name":"BLK 19 Balam Road"},
    {"ID":"15820","Name":"Norcom II Tower 4"},
    {"ID":"15821","Name":"Blk 444 Choa Chu Kang Avenue 4"},
    {"ID":"15999","Name":"BLK 654B Punggol Drive"},
    {"ID":"16367","Name":"BLK 132B Tengah Garden Avenue (Temp)"},  # replacement
    {"ID":"16004","Name":"BLK 206A Punggol Place"},
    {"ID":"16005","Name":"Woodlands 11"},
]

log = logging.getLogger("supabase-common")


def build_rows(
    api_base: str,
    loc: Dict[str, str],
    day: date,
    max_retries: int = 2,
    retry_delay: float = 2.0,
) -> List[Dict[str, object]]:
    """
    Build rows for one Singapore calendar day.

    The upstream API's start=YYYY-MM-DD parameter selects a UTC calendar
    date. A Singapore calendar day spans two UTC dates, so we fetch:

      1. The previous UTC date for 00:00-07:59 SGT
      2. The current UTC date for 08:00-23:59 SGT

    The results are combined and filtered to the exact Singapore day.
    """

    # Exact requested Singapore-day boundaries.
    sgt_start = datetime(
        day.year,
        day.month,
        day.day,
        0,
        0,
        0,
        tzinfo=SGT,
    )

    sgt_end_exclusive = sgt_start + timedelta(days=1)

    # Equivalent exact UTC range.
    utc_start = sgt_start.astimezone(timezone.utc)
    utc_end_exclusive = sgt_end_exclusive.astimezone(timezone.utc)

    # Example for 1 August SGT:
    # utc_start         = 31 July 16:00 UTC
    # utc_end_exclusive = 1 August 16:00 UTC
    previous_utc_date = utc_start.date()
    current_utc_date = utc_end_exclusive.date()

    api_dates = [
        previous_utc_date,
        current_utc_date,
    ]

    combined_raw = []

    for part_number, api_date in enumerate(api_dates, start=1):
        # Reproduce the raw website request exactly: start only, no end.
        url = (
            f"{api_base}/{loc['ID']}"
            f"?start={api_date.isoformat()}"
        )

        raw = None

        for attempt in range(max_retries + 1):
            try:
                log.info(
                    f"Fetching part {part_number}/2 for "
                    f"{loc['ID']} on SGT date {day}: "
                    f"API UTC date {api_date}"
                )

                log.debug(f"API URL: {url}")

                response = requests.get(url, timeout=30)

                # Confirmed no data for this device/API date.
                if response.status_code == 404:
                    log.warning(
                        f"Confirmed no data (404) for "
                        f"{loc['ID']} on API date {api_date}"
                    )
                    raw = []
                    break

                response.raise_for_status()

                try:
                    raw = response.json()
                except ValueError as e:
                    raise RuntimeError(
                        f"API returned invalid JSON for "
                        f"{loc['ID']} on API date {api_date}"
                    ) from e

                if not isinstance(raw, list):
                    raise RuntimeError(
                        f"Unexpected API response for {loc['ID']} "
                        f"on API date {api_date}. Expected list, "
                        f"received {type(raw).__name__}."
                    )

                break

            except requests.exceptions.RequestException as e:
                is_last_attempt = attempt == max_retries

                if not is_last_attempt:
                    log.warning(
                        f"Fetch failed for {loc['ID']} on API date "
                        f"{api_date} "
                        f"(attempt {attempt + 1}/"
                        f"{max_retries + 1}): {e}. "
                        f"Retrying in {retry_delay} seconds."
                    )
                    time.sleep(retry_delay)
                else:
                    # API/network failure is not sensor downtime.
                    raise RuntimeError(
                        f"Unable to fetch {loc['ID']} on API date "
                        f"{api_date} after {max_retries + 1} "
                        f"attempts: {e}"
                    ) from e

        if raw:
            log.info(
                f"API date {api_date} returned {len(raw)} records. "
                f"First={raw[0].get('dt')}, "
                f"last={raw[-1].get('dt')}"
            )
            combined_raw.extend(raw)
        else:
            log.warning(
                f"No readings returned for {loc['ID']} "
                f"on API date {api_date}"
            )

    # Both successful requests contained no readings.
    if not combined_raw:
        log.warning(
            f"Confirmed no sensor readings for "
            f"{loc['ID']} on SGT date {day}"
        )
        return []

    created_at = datetime.now(timezone.utc).isoformat()
    now_plus_1h_utc = datetime.now(timezone.utc) + timedelta(hours=1)

    rows_by_minute = {}
    invalid_count = 0
    outside_range_count = 0
    future_count = 0

    for item in combined_raw:
        if not isinstance(item, dict):
            invalid_count += 1
            continue

        dt_str = item.get("dt")

        if not dt_str:
            invalid_count += 1
            continue

        try:
            # The API timestamps ending with Z are genuine UTC.
            ts_utc = datetime.fromisoformat(
                dt_str.strip().replace("Z", "+00:00")
            )

            if ts_utc.tzinfo is None:
                ts_utc = ts_utc.replace(tzinfo=timezone.utc)
            else:
                ts_utc = ts_utc.astimezone(timezone.utc)

            ts_utc = ts_utc.replace(
                second=0,
                microsecond=0,
            )

        except (TypeError, ValueError) as e:
            invalid_count += 1
            log.warning(
                f"Invalid timestamp for {loc['ID']}: "
                f"{dt_str} - {e}"
            )
            continue

        # Keep only readings belonging to the requested Singapore day.
        if not (utc_start <= ts_utc < utc_end_exclusive):
            outside_range_count += 1
            continue

        if ts_utc > now_plus_1h_utc:
            future_count += 1
            continue

        value = None

        try:
            if item.get("reading") is not None:
                value = float(item["reading"])
        except (TypeError, ValueError):
            log.warning(
                f"Invalid reading for {loc['ID']} at "
                f"{dt_str}: {item.get('reading')}"
            )

        row = {
            "location_id": loc["ID"],
            "location_name": loc["Name"],
            "reading_value": value,
            "reading_datetime": ts_utc.isoformat(),
            "created_at": created_at,
        }

        # Prevent duplicate location/minute rows.
        key = (
            row["location_id"],
            row["reading_datetime"],
        )

        rows_by_minute[key] = row

    rows = sorted(
        rows_by_minute.values(),
        key=lambda row: row["reading_datetime"],
    )

    log.info(
        f"Built {len(rows)} rows for {loc['ID']} "
        f"on SGT date {day}. "
        f"Combined raw={len(combined_raw)}, "
        f"outside range={outside_range_count}, "
        f"invalid={invalid_count}, "
        f"future={future_count}"
    )

    if rows:
        first_sgt = datetime.fromisoformat(
            rows[0]["reading_datetime"]
        ).astimezone(SGT)

        last_sgt = datetime.fromisoformat(
            rows[-1]["reading_datetime"]
        ).astimezone(SGT)

        log.info(
            f"Final SGT range for {loc['ID']} on {day}: "
            f"{first_sgt.isoformat()} to "
            f"{last_sgt.isoformat()}"
        )

    return rows


def upsert_rows(supabase: Client, table: str, rows: List[Dict[str, object]]) -> int:
    """Upsert rows into Supabase in safe chunks."""
    if not rows:
        return 0

    # Deduplicate by (location_id, reading_datetime) — keep last occurrence
    seen = {}
    for row in rows:
        key = (row["location_id"], row["reading_datetime"])
        seen[key] = row  # later value overwrites earlier duplicate

    deduped = list(seen.values())
    duplicates_removed = len(rows) - len(deduped)
    if duplicates_removed > 0:
        log.info(f"  🔄 Removed {duplicates_removed} duplicate rows before upsert")

    inserted = 0
    CHUNK = 1000

    for i in range(0, len(deduped), CHUNK):
        chunk = deduped[i:i + CHUNK]
        try:
            resp = supabase.table(table).upsert(
                chunk,
                on_conflict="location_id,reading_datetime"
            ).execute()

            if isinstance(resp.data, list):
                inserted += len(resp.data)

        except Exception as e:
            log.error(f"Error upserting chunk {i}-{i+len(chunk)}: {e}")
            continue

    return inserted


def yesterday_sgt() -> date:
    """Return yesterday's date in Singapore local time (SGT)."""
    return datetime.now(SGT).date() - timedelta(days=1)
