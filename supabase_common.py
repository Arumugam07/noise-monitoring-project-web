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
"""

import logging
import os
import time
from datetime import datetime, timedelta, date, time as dtime, timezone
from typing import List, Dict

import requests
from supabase import Client

try:
    import zoneinfo  # py3.9+
except ImportError:  # pragma: no cover
    from backports import zoneinfo

SGT = zoneinfo.ZoneInfo("Asia/Singapore")
"""Singapore Timezone object used for calendar-day calculations."""

API_DEFAULT = "http://139.59.223.231:3000/api/meter-sound"
"""Default base URL for the meter-sound API (can be overridden via env)."""

# 13 device locations (ID + friendly name)
# Each item is a small dictionary:
# - "ID": the device identifier used by the API
# - "Name": a human-readable label for dashboards/queries
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
    {"ID":"16026","Name":"BLK 132B Tengah Garden Avenue"},
    {"ID":"16004","Name":"BLK 206A Punggol Place"},
    {"ID":"16005","Name":"Woodlands 11"},
]

log = logging.getLogger("supabase-common")


def build_rows(api_base: str, loc: Dict[str, str], day: date) -> List[Dict[str, object]]:
    """Build per-minute rows for a Singapore calendar day for one location.

    What it does (beginner version):
    - Calls the API for the given device `loc["ID"]` and calendar `day` (in SGT).
    - The API returns one reading per minute, starting at 00:00 SGT for that day.
    - For each minute, we generate the exact timestamp in SGT, then convert to UTC
      before storing (databases commonly use UTC for consistency).
    - Skips any readings that look too far into the future (a small safety guard).

    Parameters:
    - api_base: The base URL of the API, without the trailing device id.
    - loc: A dictionary like {"ID": "16034", "Name": "..."}.
    - day: A Python date object representing the SGT calendar day to fetch.

    Returns:
    - A list of dictionaries, one per minute, each with these keys:
      - location_id, location_name, reading_value, reading_datetime (UTC ISO string), created_at
    """
    url = f"{api_base}/{loc['ID']}?start={day.isoformat()}"
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        raw = r.json()
    except Exception as e:
        log.warning(f"Fetch failed {loc['ID']} {day}: {e}")
        return []

    if not raw:
        return []

    now_plus_1h_utc = datetime.now(timezone.utc) + timedelta(hours=1)
    rows: List[Dict[str, object]] = []
    
    for item in raw:
        # CRITICAL FIX: Use the actual timestamp from the API response
        # The API provides timestamps in the "dt" field (UTC format)
        try:
            dt_str = item.get("dt")
            if not dt_str:
                continue
            
            # Parse the ISO timestamp from API
            # Format: "2025-05-06T23:00:00.000Z"
            ts_utc = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
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
    
    return rows


def upsert_rows(supabase: Client, table: str, rows: List[Dict[str, object]]) -> int:
    """Upsert rows into Supabase in safe chunks.

    Why upsert?
    - We want to run these jobs repeatedly without creating duplicates.
    - The table has a composite unique key (location_id, reading_datetime), so
      re-writing the same minute-row will update/replace it safely.

    How it works:
    - Splits the rows into ~1000-size chunks to avoid large payload errors.
    - Uses `on_conflict="location_id,reading_datetime"` so duplicates are avoided.

    Returns:
    - An integer count of affected rows (inserted or updated) as a simple proxy.
    """
    if not rows:
        return 0
    inserted = 0
    CHUNK = 1000
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i + CHUNK]
        resp = supabase.table(table).upsert(chunk, on_conflict="location_id,reading_datetime").execute()
        if isinstance(resp.data, list):
            inserted += len(resp.data)
    return inserted


def yesterday_sgt() -> date:
    """Return yesterday's date in Singapore local time (SGT).

    We use SGT calendar days because the source API organizes data by
    local Singapore days, not by UTC days.
    """
    return datetime.now(SGT).date() - timedelta(days=1)

