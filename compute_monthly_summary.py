#!/usr/bin/env python3
"""
etl/compute_monthly_summary.py
-------------------------------
Pre-compute monthly persisted noise summaries for every location and
write them into the Supabase `monthly_persisted_summary` table.
 
Designed to run on a schedule via GitHub Actions (see
.github/workflows/monthly_summary.yml).
 
Key behaviour:
  - Incremental: skips months that are already computed and are in the past.
  - Always recomputes the current month (data is still coming in).
  - Uses SUPABASE_SERVICE_KEY (service role) so it can write to the table
    despite RLS policies. The Streamlit app uses SUPABASE_ANON_KEY (read-only).
 
Usage:
  python etl/compute_monthly_summary.py
 
Required env vars (set in GitHub Secrets or .env):
  SUPABASE_URL          – your project URL
  SUPABASE_SERVICE_KEY  – service role key (NOT the anon key)
  SUPABASE_WIDE_VIEW    – (optional) defaults to wide_view_mv
"""
 
import os
import sys
import calendar
from datetime import date
 
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
 
# ── Allow running from repo root or from etl/ sub-directory ──────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from location_presets import LOCATION_PRESETS
 
load_dotenv()
 
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_SERVICE_KEY")   # service role — needed for writes
VIEW_NAME     = os.getenv("SUPABASE_WIDE_VIEW", "wide_view_mv")
SUMMARY_TABLE = "monthly_persisted_summary"
 
# Data starts May 2025
DATA_START_YEAR  = 2025
DATA_START_MONTH = 5
 
 
# ── Supabase helpers ──────────────────────────────────────────────────────────
 
def get_client():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_SERVICE_KEY must be set.")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)
 
 
def already_computed(client, location_id: str, year: int, month: int,
                      min_db: float, max_db: float, duration_minutes: int) -> bool:
    """
    Return True if this month/location/preset combo is already in the table
    AND the month is fully in the past (so no new data can arrive).
    Current month is always recomputed.
    """
    today = date.today()
    if year == today.year and month == today.month:
        return False   # always refresh current month
 
    try:
        resp = (
            client.table(SUMMARY_TABLE)
            .select("id")
            .eq("location_id",      location_id)
            .eq("month",            str(date(year, month, 1)))
            .eq("min_db",           min_db)
            .eq("max_db",           max_db)
            .eq("duration_minutes", duration_minutes)
            .limit(1)
            .execute()
        )
        return bool(resp.data)
    except Exception:
        return False  # on error, recompute to be safe
 
 
def fetch_monthly_chunk(client, location_id: str, year: int, month: int) -> pd.DataFrame:
    """Pull all minute-level rows for one location + one calendar month."""
    _, last_day = calendar.monthrange(year, month)
    start = date(year, month, 1)
    end   = date(year, month, last_day)
 
    all_data = []
    offset = 0
    batch_size = 1000
 
    while True:
        resp = (
            client.table(VIEW_NAME)
            .select(f"Date,Time,{location_id}")
            .gte("Date", str(start))
            .lte("Date", str(end))
            .order("Date")
            .order("Time")
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        all_data.extend(batch)
        if len(batch) < batch_size:
            break
        offset += batch_size
 
    return pd.DataFrame(all_data)
 
 
def detect_incidents(df: pd.DataFrame, location_id: str,
                      min_db: float, max_db: float,
                      duration_minutes: int) -> list[dict]:
    """Detect runs of readings in [min_db, max_db] lasting ≥ duration_minutes."""
    if df.empty or location_id not in df.columns:
        return []
 
    df = df.copy()
    df[location_id] = pd.to_numeric(df[location_id], errors="coerce")
 
    vals     = df[location_id]
    in_range = vals.between(min_db, max_db, inclusive="both").fillna(False)
    group    = (in_range != in_range.shift()).cumsum().where(in_range)
 
    incidents = []
    for _gid, chunk in df.groupby(group):
        if len(chunk) < duration_minutes:
            continue
        vals_clean = chunk[location_id].dropna()
        if vals_clean.empty:
            continue
        incidents.append({
            "duration": len(chunk),
            "peak_db":  float(vals_clean.max()),
            "avg_db":   float(vals_clean.mean()),
        })
    return incidents
 
 
def upsert_summary(client, location_id: str, year: int, month: int,
                    min_db: float, max_db: float, duration_minutes: int,
                    incidents: list[dict]) -> None:
    """Write (or overwrite) a monthly summary row."""
    total_duration = sum(i["duration"] for i in incidents)
    avg_peak       = (sum(i["peak_db"] for i in incidents) / len(incidents)
                      if incidents else None)
    max_peak       = max(i["peak_db"] for i in incidents) if incidents else None
 
    record = {
        "location_id":             location_id,
        "month":                   str(date(year, month, 1)),
        "min_db":                  min_db,
        "max_db":                  max_db,
        "duration_minutes":        duration_minutes,
        "incident_count":          len(incidents),
        "total_duration_minutes":  total_duration,
        "avg_peak_db":             round(avg_peak, 2) if avg_peak is not None else None,
        "max_peak_db":             round(max_peak, 2) if max_peak is not None else None,
    }
 
    client.table(SUMMARY_TABLE).upsert(
        record,
        on_conflict="location_id,month,min_db,max_db,duration_minutes",
    ).execute()
 
 
# ── Main ──────────────────────────────────────────────────────────────────────
 
def build_month_list() -> list[tuple[int, int]]:
    today = date.today()
    months = []
    y, m = DATA_START_YEAR, DATA_START_MONTH
    while (y, m) <= (today.year, today.month):
        months.append((y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return months
 
 
def main() -> None:
    client = get_client()
    months = build_month_list()
 
    total = len(LOCATION_PRESETS) * len(months)
    done  = 0
 
    print(f"Starting ETL — {len(LOCATION_PRESETS)} locations × {len(months)} months "
          f"= {total} jobs\n")
 
    for loc_id, preset in LOCATION_PRESETS.items():
        min_db   = preset["min_db"]
        max_db   = preset["max_db"]
        duration = preset["duration_minutes"]
        name     = preset["name"]
 
        for yr, mo in months:
            done += 1
            label = date(yr, mo, 1).strftime("%b %Y")
            tag   = f"[{done:>3}/{total}] {name[:35]:<35} {label}"
 
            if already_computed(client, loc_id, yr, mo, min_db, max_db, duration):
                print(f"{tag}  → SKIP")
                continue
 
            print(f"{tag}  → fetching…", end="", flush=True)
            try:
                df        = fetch_monthly_chunk(client, loc_id, yr, mo)
                incidents = detect_incidents(df, loc_id, min_db, max_db, duration)
                upsert_summary(client, loc_id, yr, mo,
                                min_db, max_db, duration, incidents)
                print(f" ✓  {len(incidents)} incidents")
            except Exception as exc:
                print(f" ERROR: {exc}")
 
    print("\n✅ ETL complete.")
 
 
if __name__ == "__main__":
    main()
