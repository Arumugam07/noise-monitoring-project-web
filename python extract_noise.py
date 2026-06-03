#!/usr/bin/env python3
"""
Extract noise readings from Supabase: 2 Jun 2025 → 2 Jun 2026
Rows = timestamps, Columns = 13 locations
No row limit — fetches everything in batches
"""

import os, io
from datetime import date
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

load_dotenv()

# ── Config — edit these ────────────────────────────────────────────────────
SUPABASE_URL  = os.getenv("SUPABASE_URL",  "YOUR_SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_ANON_KEY", "YOUR_SUPABASE_ANON_KEY")
VIEW_NAME     = "wide_view_mv"   # your materialized view
OUTPUT_FILE   = "noise_2025_2026.xlsx"
START_DATE    = "2025-06-02"
END_DATE      = "2026-06-02"
BATCH_SIZE    = 5000
# ──────────────────────────────────────────────────────────────────────────

LOCATION_IDS = [
    "15490","16034","16041","14542","15725","16032",
    "16045","15820","15821","15999","16026","16004","16005"
]
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

def fetch_all():
    client = create_client(SUPABASE_URL, SUPABASE_KEY)
    all_rows, offset = [], 0
    select = "Date,Time," + ",".join(LOCATION_IDS)

    print(f"Fetching {START_DATE} → {END_DATE} ...")
    while True:
        resp = (
            client.table(VIEW_NAME)
            .select(select)
            .gte("Date", START_DATE)
            .lte("Date", END_DATE)
            .order("Date").order("Time")
            .range(offset, offset + BATCH_SIZE - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        all_rows.extend(batch)
        print(f"  {len(all_rows):,} rows fetched...", end="\r")
        if len(batch) < BATCH_SIZE:
            break
        offset += BATCH_SIZE

    print(f"\nDone — {len(all_rows):,} rows total")
    return all_rows

def build_df(rows):
    df = pd.DataFrame(rows)
    # Format Date → "2 Jun 25"
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%-d %b %y")
    # Format Time → "08:00"
    df["Time"] = df["Time"].astype(str).str[:5]
    # Rename IDs → friendly names
    df = df.rename(columns=LOCATION_NAMES)
    # Convert to numeric
    for name in LOCATION_NAMES.values():
        if name in df.columns:
            df[name] = pd.to_numeric(df[name], errors="coerce")
    # Column order
    cols = ["Date","Time"] + [LOCATION_NAMES[i] for i in LOCATION_IDS if LOCATION_NAMES[i] in df.columns]
    return df[cols]

def db_color(val):
    if pd.isna(val):   return None
    if val < 50:       return "D4EDDA"   # green
    elif val < 70:     return "FFF3CD"   # yellow
    elif val < 85:     return "FFE5D0"   # orange
    else:              return "F8D7DA"   # red

def write_excel(df):
    print(f"Writing Excel ({len(df):,} rows) — this may take a few minutes...")
    wb = Workbook(write_only=True)   # write_only = much faster for large files
    ws = wb.create_sheet("Noise Readings")

    hdr_font   = Font(name="Arial", bold=True, color="FFFFFF", size=10)
    hdr_fill   = PatternFill("solid", start_color="1F77B4")
    date_fill  = PatternFill("solid", start_color="EBF3FB")
    thin       = Side(style="thin", color="CCCCCC")
    border     = Border(left=thin, right=thin, bottom=thin)
    data_font  = Font(name="Arial", size=9)
    center     = Alignment(horizontal="center", vertical="center")
    left       = Alignment(horizontal="left",   vertical="center")

    from openpyxl.cell import WriteOnlyCell

    # Header row
    headers = list(df.columns)
    hdr_cells = []
    for h in headers:
        c = WriteOnlyCell(ws, value=h)
        c.font = hdr_font; c.fill = hdr_fill; c.alignment = center; c.border = border
        hdr_cells.append(c)
    ws.append(hdr_cells)

    # Set column widths (write_only workaround)
    from openpyxl.utils import get_column_letter
    ws.column_dimensions[get_column_letter(1)].width = 11
    ws.column_dimensions[get_column_letter(2)].width = 7
    for ci in range(3, len(headers)+1):
        ws.column_dimensions[get_column_letter(ci)].width = 24

    # Data rows
    for i, row in enumerate(df.itertuples(index=False), 1):
        cells = []
        for ci, val in enumerate(row):
            c = WriteOnlyCell(ws, value=val if not pd.isna(val) else None)
            c.font = data_font; c.border = border
            if ci == 0:
                c.fill = date_fill; c.alignment = left
            elif ci == 1:
                c.alignment = center
            else:
                c.alignment = center
                color = db_color(val)
                if color:
                    c.fill = PatternFill("solid", start_color=color)
            cells.append(c)
        ws.append(cells)

        if i % 100000 == 0:
            print(f"  Written {i:,} rows...")

    wb.save(OUTPUT_FILE)
    print(f"\n✅ Saved: {OUTPUT_FILE}")
    print(f"   Rows: {len(df):,}  |  Columns: {len(df.columns)}")

if __name__ == "__main__":
    rows = fetch_all()
    df   = build_df(rows)
    write_excel(df)
