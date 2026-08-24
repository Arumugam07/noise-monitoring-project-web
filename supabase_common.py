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
    Fetch one device-day using the same request as the raw API website.

    The working raw website sends only:
        /meter-sound/{location_id}?start=YYYY-MM-DD

    Do not add an end parameter because the API returns a different,
    incomplete range when end is included.
    """

    # Reproduce the raw website's request exactly.
    url = (
        f"{api_base}/{loc['ID']}"
        f"?start={day.isoformat()}"
    )

    raw = None

    for attempt in range(max_retries + 1):
        try:
            log.info(
                f"Fetching {loc['ID']} on {day} "
                f"(attempt {attempt + 1}/{max_retries + 1})"
            )
            log.debug(f"API URL: {url}")

            response = requests.get(url, timeout=30)

            # Confirmed no data for this sensor/day.
            if response.status_code == 404:
                log.warning(
                    f"Confirmed no data (404) for "
                    f"{loc['ID']} on {day}"
                )
                return []

            response.raise_for_status()

            try:
                raw = response.json()
            except ValueError as e:
                raise RuntimeError(
                    f"API returned invalid JSON for "
                    f"{loc['ID']} on {day}"
                ) from e

            if not isinstance(raw, list):
                raise RuntimeError(
                    f"Unexpected API response for {loc['ID']} on {day}. "
                    f"Expected list, received {type(raw).__name__}."
                )

            break

        except requests.exceptions.RequestException as e:
            is_last_attempt = attempt == max_retries

            if not is_last_attempt:
                log.warning(
                    f"Fetch failed for {loc['ID']} on {day} "
                    f"(attempt {attempt + 1}/{max_retries + 1}): {e}. "
                    f"Retrying in {retry_delay} seconds."
                )
                time.sleep(retry_delay)
            else:
                # Fetch failure is not confirmed sensor downtime.
                raise RuntimeError(
                    f"Unable to fetch {loc['ID']} on {day} after "
                    f"{max_retries + 1} attempts: {e}"
                ) from e

    # HTTP 200 with an empty list means confirmed no readings.
    if not raw:
        log.warning(
            f"Confirmed no data in API response for "
            f"{loc['ID']} on {day}"
        )
        return []

    log.info(
        f"API returned {len(raw)} records for "
        f"{loc['ID']} on {day}"
    )

    log.info(
        f"Raw timestamp range: "
        f"{raw[0].get('dt')} to {raw[-1].get('dt')}"
    )

    created_at = datetime.now(timezone.utc).isoformat()
    now_plus_1h_utc = datetime.now(timezone.utc) + timedelta(hours=1)

    rows_by_minute = {}
    invalid_count = 0
    wrong_date_count = 0
    future_count = 0

    for item in raw:
        if not isinstance(item, dict):
            invalid_count += 1
            continue

        dt_str = item.get("dt")

        if not dt_str:
            invalid_count += 1
            continue

        try:
            cleaned_dt = dt_str.strip()

            # The raw website associates the timestamp's displayed date/time
            # directly with the selected Singapore calendar date. Remove Z,
            # assign Singapore timezone, then convert to UTC for Supabase.
            if cleaned_dt.endswith("Z"):
                cleaned_dt = cleaned_dt[:-1]

            source_sgt = datetime.fromisoformat(cleaned_dt)

            if source_sgt.tzinfo is not None:
                source_sgt = source_sgt.replace(tzinfo=None)

            source_sgt = source_sgt.replace(tzinfo=SGT)

            ts_utc = (
                source_sgt
                .astimezone(timezone.utc)
                .replace(second=0, microsecond=0)
            )

        except (TypeError, ValueError) as e:
            invalid_count += 1
            log.warning(
                f"Invalid timestamp for {loc['ID']}: "
                f"{dt_str} - {e}"
            )
            continue

        # Confirm that the API reading belongs to the requested date.
        if ts_utc.astimezone(SGT).date() != day:
            wrong_date_count += 1
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

        key = (
            row["location_id"],
            row["reading_datetime"],
        )

        rows_by_minute[key] = row

    rows = sorted(
        rows_by_minute.values(),
        key=lambda row: row["reading_datetime"],
    )

    duplicates_removed = (
        len(raw)
        - invalid_count
        - wrong_date_count
        - future_count
        - len(rows)
    )

    log.info(
        f"Built {len(rows)} rows for {loc['ID']} on {day}. "
        f"Raw={len(raw)}, "
        f"duplicates_removed={max(duplicates_removed, 0)}, "
        f"invalid={invalid_count}, "
        f"wrong_date={wrong_date_count}, "
        f"future={future_count}"
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
