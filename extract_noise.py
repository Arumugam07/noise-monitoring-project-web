"""
Extract noise data Apr 2025 – May 2026 from meter_readings.
Strategy: fetch one location × one month at a time → each request is small and fast.
Assembles everything in memory, then writes one Excel with monthly tabs.
"""
import os
import time
import requests
import pandas as pd
from datetime import date, timedelta
from calendar import monthrange

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://tgznxzfdlxhxqwpohhyl.supabase.co")
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

TABLE       = "meter_readings"
OUTPUT_FILE = "noise_1_apr_2025_to_31_may_2026.xlsx"
PAGE_SIZE   = 5000

# Months to export  (year, month)
MONTHS = [
    (2025, 4), (2025, 5), (2025, 6), (2025, 7), (2025, 8), (2025, 9),
    (2025, 10), (2025, 11), (2025, 12),
    (2026, 1), (2026, 2), (2026, 3), (2026, 4), (2026, 5),
]

LOCATION_ID_TO_NAME = {
    "15490": "Singapore Sports School",
    "16034": "BLK 120 Serangoon North Ave 1",
    "16041": "BLK 838 Hougang Central",
    "14542": "BLK 558 Jurong West Street 42",
    "15725": "Jurong Safra, Block C",
    "16032": "AMA KENG SITE",
    "16045": "BLK 19 Balam Road",
    "15820": "Norcom II Tower 4",
    "15821": "Blk 444 Choa Chu Kang Avenue 4",
    "15999": "BLK 654B Punggol Drive",
    "16367": "BLK 132B Tengah Garden Avenue",
    "16004": "BLK 206A Punggol Place",
    "16005": "Woodlands 11",
}

HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}


def fetch_location_month(location_id, year, month):
    """Fetch all rows for one location in one month. Paginated."""
    last_day = monthrange(year, month)[1]
    date_from = f"{year:04d}-{month:02d}-01T00:00:00+08:00"
    date_to   = f"{year:04d}-{month:02d}-{last_day:02d}T23:59:59+08:00"

    all_rows = []
    offset   = 0

    while True:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/{TABLE}",
            headers=HEADERS,
            params=[
                ("select",           "reading_datetime,reading_value"),
                ("location_id",      f"eq.{location_id}"),
                ("reading_datetime", f"gte.{date_from}"),
                ("reading_datetime", f"lte.{date_to}"),
                ("order",            "reading_datetime.asc"),
                ("limit",            PAGE_SIZE),
                ("offset",           offset),
            ],
            timeout=60,
        )

        if resp.status_code >= 400:
            print(f"    ERROR {resp.status_code}: {resp.text[:200]}")
            return []

        rows = resp.json()
        all_rows.extend(rows)

        if len(rows) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
        time.sleep(0.05)

    return all_rows


def build_month_dataframe(year, month):
    """Fetch all locations for one month and return a wide dataframe."""
    last_day  = monthrange(year, month)[1]
    month_dfs = []

    print(f"\n  [{year}-{month:02d}] Fetching {len(LOCATION_ID_TO_NAME)} locations...")

    for loc_id, loc_name in LOCATION_ID_TO_NAME.items():
        rows = fetch_location_month(loc_id, year, month)
        print(f"    {loc_name[:35]:35s} {len(rows):5,} rows")

        if not rows:
            continue

        df = pd.DataFrame(rows)
        df["reading_datetime"] = pd.to_datetime(df["reading_datetime"], utc=True)
        df["reading_datetime"] = (df["reading_datetime"]
                                  .dt.tz_convert("Asia/Singapore")
                                  .dt.tz_localize(None))
        df["reading_value"]    = pd.to_numeric(df["reading_value"], errors="coerce")
        df["minute"]           = df["reading_datetime"].dt.floor("min")
        df                     = df.rename(columns={"reading_value": loc_name})
        df                     = df[["minute", loc_name]]
        month_dfs.append(df)

    if not month_dfs:
        return pd.DataFrame()

    # Merge all locations on minute
    wide = month_dfs[0]
    for df in month_dfs[1:]:
        wide = pd.merge(wide, df, on="minute", how="outer")

    wide = wide.sort_values("minute").reset_index(drop=True)

    # Reorder sensor columns to match LOCATION_ID_TO_NAME order
    sensor_cols = [n for n in LOCATION_ID_TO_NAME.values() if n in wide.columns]
    wide        = wide[["minute"] + sensor_cols]

    wide.insert(0, "Date", wide["minute"].dt.strftime("%d %b %y").str.upper())
    wide.insert(1, "Time", wide["minute"].dt.strftime("%H:%M"))
    wide = wide.drop(columns=["minute"])

    return wide


def write_excel(month_frames):
    total_rows = sum(len(df) for df in month_frames.values())
    print(f"\nWriting Excel: {total_rows:,} total rows across {len(month_frames)} tabs...")

    with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
        workbook = writer.book

        header_fmt = workbook.add_format({
            "font_name": "Segoe UI", "font_size": 11, "bold": True,
            "font_color": "#FFFFFF", "bg_color": "#1F4E78",
            "align": "center", "valign": "vcenter",
            "border": 1, "border_color": "#D9D9D9",
        })
        center_fmt = workbook.add_format({
            "font_name": "Segoe UI", "font_size": 10,
            "align": "center", "valign": "vcenter",
            "border": 1, "border_color": "#D9D9D9",
        })
        numeric_fmt = workbook.add_format({
            "font_name": "Segoe UI", "font_size": 10,
            "num_format": "0.00", "align": "right", "valign": "vcenter",
            "border": 1, "border_color": "#D9D9D9",
        })

        for (year, month), df in month_frames.items():
            if df.empty:
                print(f"  Skipping {year}-{month:02d} (no data)")
                continue

            tab = date(year, month, 1).strftime("%b %Y")
            df.to_excel(writer, sheet_name=tab, index=False, na_rep="")
            ws     = writer.sheets[tab]
            n_rows = len(df)
            n_cols = len(df.columns)

            ws.hide_gridlines(0)
            for ci, cname in enumerate(df.columns):
                ws.write(0, ci, cname, header_fmt)
            ws.set_row(0, 26)
            ws.set_column(0, 1, 14, center_fmt)
            ws.set_column(2, n_cols - 1, 20, numeric_fmt)
            ws.freeze_panes(1, 2)
            ws.autofilter(0, 0, n_rows, n_cols - 1)

            for ci, cname in enumerate(df.columns):
                max_len = df[cname].astype(str).head(500).str.len().max()
                ws.set_column(ci, ci, max(int(max_len) + 4, 14))

            print(f"  Wrote tab: {tab} ({n_rows:,} rows)")

    print(f"\nSaved: {OUTPUT_FILE}")


def main():
    print(f"Exporting Apr 2025 → May 2026  ({len(MONTHS)} months × {len(LOCATION_ID_TO_NAME)} locations)")

    month_frames = {}
    for year, month in MONTHS:
        df = build_month_dataframe(year, month)
        month_frames[(year, month)] = df

    write_excel(month_frames)


if __name__ == "__main__":
    main()
