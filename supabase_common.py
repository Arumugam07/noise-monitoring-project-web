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
    Fetch one complete Singapore calendar day.

    The Singapore day is split into separate UTC-date requests because
    the upstream API does not reliably return data across UTC midnight.
    Each request includes one extra hour because the API has been observed
    to omit its final requested hour. Results are filtered back to the
    exact requested Singapore-day UTC range.
    """

    # Exact Singapore calendar-day boundaries.
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

    # Exact UTC range required.
    utc_start = sgt_start.astimezone(timezone.utc)
    utc_end_exclusive = sgt_end_exclusive.astimezone(timezone.utc)

    # Split at UTC midnight.
    utc_midnight = datetime(
        utc_end_exclusive.year,
        utc_end_exclusive.month,
        utc_end_exclusive.day,
        0,
        0,
        0,
        tzinfo=timezone.utc,
    )

    request_ranges = [
        # First UTC-date portion, with one-hour end padding.
        (
            utc_start,
            utc_midnight + timedelta(hours=1),
        ),

        # Second UTC-date portion, with one-hour end padding.
        (
            utc_midnight,
            utc_end_exclusive + timedelta(hours=1),
        ),
    ]

    combined_raw = []

    for part_number, (request_start, request_end) in enumerate(
        request_ranges,
        start=1,
    ):
        start_str = request_start.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = request_end.strftime("%Y-%m-%dT%H:%M:%S")

        url = (
            f"{api_base}/{loc['ID']}"
            f"?start={start_str}&end={end_str}"
        )

        log.info(
            f"Fetching part {part_number}/2 for {loc['ID']} on {day}: "
            f"{start_str} to {end_str}"
        )

        raw = None

        for attempt in range(max_retries + 1):
            try:
                response = requests.get(url, timeout=30)

                if response.status_code == 404:
                    log.warning(
                        f"No data (404) for part {part_number}/2, "
                        f"location {loc['ID']} on {day}"
                    )
                    raw = []
                    break

                response.raise_for_status()

                try:
                    raw = response.json()
                except ValueError as e:
                    raise RuntimeError(
                        f"Invalid JSON for {loc['ID']} on {day}, "
                        f"part {part_number}/2"
                    ) from e

                if not isinstance(raw, list):
                    raise RuntimeError(
                        f"Unexpected API response for {loc['ID']} on {day}, "
                        f"part {part_number}/2: expected list, "
                        f"received {type(raw).__name__}"
                    )

                break

            except requests.exceptions.RequestException as e:
                is_last_attempt = attempt == max_retries

                if not is_last_attempt:
                    log.warning(
                        f"Fetch failed for {loc['ID']} on {day}, "
                        f"part {part_number}/2 "
                        f"(attempt {attempt + 1}/{max_retries + 1}): {e}. "
                        f"Retrying in {retry_delay} seconds."
                    )
                    time.sleep(retry_delay)
                else:
                    raise RuntimeError(
                        f"Unable to fetch {loc['ID']} on {day}, "
                        f"part {part_number}/2 after "
                        f"{max_retries + 1} attempts: {e}"
                    ) from e

        if raw:
            log.info(
                f"Part {part_number}/2 returned {len(raw)} raw records. "
                f"First={raw[0].get('dt')}, "
                f"last={raw[-1].get('dt')}"
            )
            combined_raw.extend(raw)
        else:
            log.warning(
                f"Part {part_number}/2 returned no readings for "
                f"{loc['ID']} on {day}"
            )

    if not combined_raw:
        log.warning(
            f"Confirmed no readings for {loc['ID']} on {day}"
        )
        return []

    now_plus_1h_utc = datetime.now(timezone.utc) + timedelta(hours=1)
    created_at = datetime.now(timezone.utc).isoformat()

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

            ts_utc = ts_utc.replace(second=0, microsecond=0)

        except (TypeError, ValueError) as e:
            invalid_count += 1
            log.warning(
                f"Invalid timestamp for {loc['ID']}: "
                f"{dt_str} - {e}"
            )
            continue

        # Remove the extra padding and keep only the requested SGT day.
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
                f"Invalid reading for {loc['ID']} at {dt_str}: "
                f"{item.get('reading')}"
            )

        row = {
            "location_id": loc["ID"],
            "location_name": loc["Name"],
            "reading_value": value,
            "reading_datetime": ts_utc.isoformat(),
            "created_at": created_at,
        }

        # Deduplicate overlapping/padded results.
        key = (loc["ID"], ts_utc.isoformat())
        rows_by_minute[key] = row

    rows = sorted(
        rows_by_minute.values(),
        key=lambda row: row["reading_datetime"],
    )

    log.info(
        f"Built {len(rows)} rows for {loc['ID']} on {day}. "
        f"Combined raw={len(combined_raw)}, "
        f"outside exact range={outside_range_count}, "
        f"invalid={invalid_count}, "
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
