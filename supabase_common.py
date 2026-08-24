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
    Fetch and build per-minute rows for one complete Singapore calendar day.

    The API query parameters are interpreted as UTC. Therefore, the requested
    Singapore day must first be converted into its equivalent UTC range.

    Example for 1 August 2026:
        Singapore: 2026-08-01 00:00:00 to 23:59:59
        UTC:       2026-07-31 16:00:00 to 2026-08-01 15:59:59

    API timestamps ending with "Z" are parsed as genuine UTC timestamps.
    """

    # Build the beginning and end of the requested Singapore calendar day.
    sgt_start = datetime(
        day.year,
        day.month,
        day.day,
        0,
        0,
        0,
        tzinfo=SGT,
    )

    sgt_end = sgt_start + timedelta(days=1) - timedelta(seconds=1)

    # Convert the Singapore day boundaries to UTC for the API request.
    utc_start = sgt_start.astimezone(timezone.utc)
    utc_end = sgt_end.astimezone(timezone.utc)

    # The API expects naive-looking query parameters representing UTC.
    start_str = utc_start.strftime("%Y-%m-%dT%H:%M:%S")
    end_str = utc_end.strftime("%Y-%m-%dT%H:%M:%S")

    url = (
        f"{api_base}/{loc['ID']}"
        f"?start={start_str}&end={end_str}"
    )

    log.info(
        f"Fetching {loc['ID']} for SGT date {day}: "
        f"UTC {start_str} to {end_str}"
    )

    raw = None

    for attempt in range(max_retries + 1):
        try:
            log.debug(
                f"Fetching: {url} "
                f"(attempt {attempt + 1}/{max_retries + 1})"
            )

            response = requests.get(url, timeout=30)

            # A real 404 is treated as confirmed no sensor data.
            if response.status_code == 404:
                log.warning(
                    f"Confirmed no data (404) for "
                    f"{loc['ID']} on {day}"
                )
                return []

            # Raises an exception for 4xx or 5xx responses.
            response.raise_for_status()

            try:
                raw = response.json()
            except ValueError as e:
                raise RuntimeError(
                    f"API returned invalid JSON for "
                    f"{loc['ID']} on {day}"
                ) from e

            # Validate the expected API response structure.
            if not isinstance(raw, list):
                raise RuntimeError(
                    f"Unexpected API response for {loc['ID']} on {day}. "
                    f"Expected a list but received "
                    f"{type(raw).__name__}."
                )

            # API request completed successfully.
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
                # A network/API failure is not confirmed sensor downtime.
                raise RuntimeError(
                    f"Unable to fetch {loc['ID']} on {day} after "
                    f"{max_retries + 1} attempts: {e}"
                ) from e

    # HTTP 200 with an empty list means confirmed no sensor readings.
    if not raw:
        log.warning(
            f"Confirmed no data in successful API response for "
            f"{loc['ID']} on {day}"
        )
        return []

    log.info(
        f"API returned {len(raw)} raw records for "
        f"{loc['ID']} on {day}"
    )

    # Helpful diagnostic information for checking API boundaries.
    first_raw_dt = raw[0].get("dt") if isinstance(raw[0], dict) else None
    last_raw_dt = raw[-1].get("dt") if isinstance(raw[-1], dict) else None

    log.info(
        f"Raw API timestamp range for {loc['ID']} on {day}: "
        f"{first_raw_dt} to {last_raw_dt}"
    )

    now_plus_1h_utc = datetime.now(timezone.utc) + timedelta(hours=1)
    created_at = datetime.now(timezone.utc).isoformat()

    rows: List[Dict[str, object]] = []
    invalid_timestamp_count = 0
    outside_day_count = 0
    future_timestamp_count = 0

    for item in raw:
        if not isinstance(item, dict):
            log.warning(
                f"Skipping unexpected non-object API item for "
                f"{loc['ID']} on {day}: {item}"
            )
            continue

        try:
            dt_str = item.get("dt")

            if not dt_str:
                invalid_timestamp_count += 1
                continue

            # The API's trailing Z represents UTC.
            ts_utc = datetime.fromisoformat(
                dt_str.strip().replace("Z", "+00:00")
            )

            # Ensure the parsed timestamp is timezone-aware and UTC.
            if ts_utc.tzinfo is None:
                ts_utc = ts_utc.replace(tzinfo=timezone.utc)
            else:
                ts_utc = ts_utc.astimezone(timezone.utc)

            # Store one timestamp per minute.
            ts_utc = ts_utc.replace(second=0, microsecond=0)

        except (TypeError, ValueError) as e:
            invalid_timestamp_count += 1
            log.warning(
                f"Invalid timestamp for {loc['ID']}: "
                f"{item.get('dt')} - {e}"
            )
            continue

        # Convert back to SGT to confirm the reading belongs to the
        # Singapore calendar day that was requested.
        ts_sgt = ts_utc.astimezone(SGT)

        if ts_sgt.date() != day:
            outside_day_count += 1
            log.warning(
                f"Skipping out-of-day reading for {loc['ID']}: "
                f"raw={item.get('dt')}, "
                f"SGT={ts_sgt.isoformat()}, "
                f"requested_day={day}"
            )
            continue

        # Safety check against unexpectedly future-dated readings.
        if ts_utc > now_plus_1h_utc:
            future_timestamp_count += 1
            continue

        # Extract the reading value.
        value = None

        try:
            if item.get("reading") is not None:
                value = float(item["reading"])
        except (TypeError, ValueError):
            log.warning(
                f"Invalid reading value for {loc['ID']} at "
                f"{item.get('dt')}: {item.get('reading')}"
            )
            value = None

        rows.append({
            "location_id": loc["ID"],
            "location_name": loc["Name"],
            "reading_value": value,
            "reading_datetime": ts_utc.isoformat(),
            "created_at": created_at,
        })

    # Deduplicate the API response at minute level.
    # Later entries replace earlier entries for the same location/minute.
    deduplicated_rows = {}

    for row in rows:
        key = (
            row["location_id"],
            row["reading_datetime"],
        )
        deduplicated_rows[key] = row

    final_rows = list(deduplicated_rows.values())
    duplicates_removed = len(rows) - len(final_rows)

    # Sort chronologically for predictable output and upserts.
    final_rows.sort(key=lambda row: row["reading_datetime"])

    log.info(
        f"Built {len(final_rows)} rows for {loc['ID']} on {day}. "
        f"Raw={len(raw)}, "
        f"duplicates_removed={duplicates_removed}, "
        f"invalid_timestamps={invalid_timestamp_count}, "
        f"outside_requested_day={outside_day_count}, "
        f"future_timestamps={future_timestamp_count}"
    )

    return final_rows


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
