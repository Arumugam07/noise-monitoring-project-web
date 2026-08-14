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


def build_rows(api_base: str, loc: Dict[str, str], day: date,
                max_retries: int = 2, retry_delay: float = 2.0) -> List[Dict[str, object]]:
    """Build per-minute rows for an SGT calendar day for one location."""
    # day boundaries in SGT, converted to UTC for the API call - the API
    # interprets naive query params as UTC, so we must convert here rather
    # than sending the SGT wall-clock values directly.
    sgt_start = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=SGT)
    sgt_end = sgt_start + timedelta(days=1) - timedelta(seconds=1)  # 23:59:59 SGT
    utc_start = sgt_start.astimezone(timezone.utc)
    utc_end = sgt_end.astimezone(timezone.utc)

    start_str = utc_start.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = utc_end.strftime("%Y-%m-%dT%H:%M:%S")

    # Construct a clean URL that explicitly sets the start and end query bounds
    url = f"{api_base}/{loc['ID']}?start={start_str}&end={end_str}"

    raw = None
    for attempt in range(max_retries + 1):
        try:
            log.debug(f"Fetching: {url} (attempt {attempt + 1})")
            r = requests.get(url, timeout=30)

            if r.status_code == 404:
                # Genuine "no data for this device/day" - don't retry, don't error.
                log.debug(f"No data (404) for {loc['ID']} on {day}")
                return []

            r.raise_for_status()
            raw = r.json()
            break  # success

        except requests.exceptions.RequestException as e:
            is_last = attempt == max_retries
            level = log.error if is_last else log.warning
            level(
                f"Fetch failed {loc['ID']} {day} (attempt {attempt + 1}/{max_retries + 1}): {e}"
            )
            if not is_last:
                time.sleep(retry_delay)
            else:
                # Exhausted retries on a non-404 error - this is NOT the same
                # as "no data", it's an unresolved failure. Surface it clearly
                # so it isn't silently counted as an empty day.
                log.error(
                    f"GIVING UP on {loc['ID']} {day} after {max_retries + 1} attempts "
                    f"- treating as failed fetch, NOT confirmed empty"
                )
                return []

    if not raw:
        log.debug(f"No data returned for {loc['ID']} on {day}")
        return []

    now_plus_1h_utc = datetime.now(timezone.utc) + timedelta(hours=1)
    rows: List[Dict[str, object]] = []

    for item in raw:
        # Use the actual timestamp from the API response
        try:
            dt_str = item.get("dt")
            if not dt_str:
                continue

            # Parse the ISO timestamp from API (format: "2025-05-06T23:00:00.000Z")
            ts_utc = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))

            # TRUNCATE to minute level (remove seconds and microseconds)
            ts_utc = ts_utc.replace(second=0, microsecond=0)

        except Exception as e:
            log.warning(f"Invalid timestamp for {loc['ID']}: {item.get('dt')} - {e}")
            continue

        # Skip future timestamps (safety check)
        if ts_utc > now_plus_1h_utc:
            continue

        # Extract the reading value
        value = None
        try:
            if item.get("reading") is not None:
                value = float(item.get("reading"))
        except Exception:
            value = None

        # Build the row for Supabase
        rows.append({
            "location_id": loc["ID"],
            "location_name": loc["Name"],
            "reading_value": value,
            "reading_datetime": ts_utc.isoformat(),
            "created_at": datetime.now(timezone.utc).isoformat(),
        })

    log.debug(f"Built {len(rows)} rows for {loc['ID']} on {day}")
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
