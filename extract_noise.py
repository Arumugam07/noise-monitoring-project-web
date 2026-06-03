#!/usr/bin/env python3
"""
Extract noise readings from Supabase: 2 Jun 2025 → 2 Jun 2026
Queries meter_readings directly, pivots in Python, exports to Excel.
"""

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.cell import WriteOnlyCell

load_dotenv()

# ✅ Load from environment (GitHub Actions will supply these)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

OUTPUT_FILE  = "noise_2025_2026.xlsx"
START_DT     = "2025-06-02T00:00:00"
END_DT       = "2026-06-02T23:59:59"
BATCH_SIZE   = 10000

# ✅ Safety check (prevents your previous error)
if not SUPABASE_URL or "supabase.co" not in SUPABASE_URL:
    raise ValueError("Invalid SUPABASE_URL. Must be your project URL (e.g. https://xxxx.supabase.co)")

if not SUPABASE_KEY:
    raise ValueError("Missing SUPABASE_ANON_KEY")

LOCATION_NAMES = {
    "15490": "Singapore Sports School",
    "16034": "BLK 120 Serangoon North Ave 1",
    "16041": "BLK 838 Hougang Central",
    "14542": "BLK 558 Jurong West Street 42",
    "15725": "Jurong Safra Block C",
    "16032": "AMA KENG SITE",
    "16045": "BLK 19 Balam Road",
    "15820": "Norcom II Tower 4",
    "15821": "Blk 444 Choa Chu Kang Ave 4",
    "15999": "BLK 654B Punggol Drive",
    "16026": "BLK 132B Tengah Garden Avenue",
    "16004": "BLK 206A Punggol Place",
    "16005": "Woodlands 11",
}
LOCATION_IDS = list(LOCATION_NAMES.keys())


def fetch_all():
    client = create_client(SUPABASE_URL, SUPABASE_KEY)

    all_rows = []
    offset = 0

    print(f"Fetching data: {START_DT} → {END_DT}")

    while True:
        resp = (
            client.table("meter_readings")
            .select("reading_datetime,location_id,reading_value")
            .gte("reading_datetime", START_DT)
            .lte("reading_datetime", END_DT)
            .in_("location_id", LOCATION_IDS)
            .order("reading_datetime", desc=False)
            .range(offset, offset + BATCH_SIZE - 1)
            .execute()
        )

        batch = resp.data or []
        if not batch:
            print(f"No more data at offset {offset}")
            break

        all_rows.extend(batch)
        print(f"Fetched {len(all_rows):,} rows", end="\r")

        if len(batch) < BATCH_SIZE:
            break

        offset += BATCH_SIZE

    print(f"\nTotal rows fetched: {len(all_rows):,}")
    return all_rows


def build_pivot(rows):
    if not rows:
        raise ValueError("No data returned.")

    df = pd.DataFrame(rows)

    df["reading_datetime"] = pd.to_datetime(df["reading_datetime"], utc=True)
    df["reading_datetime"] = df["reading_datetime"].dt.tz_convert("Asia/Singapore")

    df["Date"] = df["reading_datetime"].dt.strftime("%d %b %y")
    df["Time"] = df["reading_datetime"].dt.strftime("%H:%M")
    df["dt_key"] = df["reading_datetime"].dt.floor("min")

    df["reading_value"] = pd.to_numeric(df["reading_value"], errors="coerce")

    pivot = df.pivot_table(
        index=["dt_key", "Date", "Time"],
        columns="location_id",
        values="reading_value",
        aggfunc="max"
    ).reset_index()

    pivot = pivot.sort_values("dt_key").drop(columns=["dt_key"])
    pivot.columns.name = None

    pivot = pivot.rename(columns={lid: LOCATION_NAMES[lid] for lid in LOCATION_IDS if lid in pivot.columns})

    loc_cols = [LOCATION_NAMES[lid] for lid in LOCATION_IDS if LOCATION_NAMES[lid] in pivot.columns]
    pivot = pivot[["Date", "Time"] + loc_cols]

    print(f"Pivot built: {len(pivot):,} rows")
    return pivot


def write_excel(df):
    print(f"Writing {OUTPUT_FILE}...")

    wb = Workbook(write_only=True)
    ws = wb.create_sheet("Noise Readings")

    hdr_font = Font(bold=True, color="FFFFFF")
    hdr_fill = PatternFill("solid", start_color="1F77B4")

    for col in df.columns:
        cell = WriteOnlyCell(ws, value=col)
        cell.font = hdr_font
        cell.fill = hdr_fill
        ws.append([cell] if col == df.columns[0] else ws._cells[-1] + [cell])

    for row in df.itertuples(index=False):
        ws.append(list(row))

    wb.save(OUTPUT_FILE)
    print("Excel export done.")


if __name__ == "__main__":
    data = fetch_all()
    df = build_pivot(data)
    write_excel(df)
