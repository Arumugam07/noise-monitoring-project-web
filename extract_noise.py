"""
Extract noise data Apr 2025 – May 2026 from meter_readings directly.
Avoids the view-level statement timeout by fetching raw rows and pivoting in Python.
"""
import os
import time
import requests
import pandas as pd

SUPABASE_URL = os.environ.get(
    "SUPABASE_URL",
    "https://tgznxzfdlxhxqwpohhyl.supabase.co",
)
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

TABLE       = "meter_readings"
OUTPUT_FILE = "noise_1_apr_2025_to_31_may_2026.xlsx"
PAGE_SIZE   = 5000

DATE_FROM = "2025-04-01T00:00:00+08:00"
DATE_TO   = "2026-06-01T00:00:00+08:00"   # exclusive upper bound

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


def fetch_all_rows():
    all_rows = []
    offset   = 0
    headers  = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

    while True:
        print(f"  Fetching rows {offset:,} – {offset + PAGE_SIZE - 1:,} ...", end=" ")

        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/{TABLE}",
            headers=headers,
            params=[
                ("select",           "reading_datetime,location_id,reading_value"),
                ("reading_datetime", f"gte.{DATE_FROM}"),
                ("reading_datetime", f"lt.{DATE_TO}"),
                ("order",            "reading_datetime.asc"),
                ("limit",            PAGE_SIZE),
                ("offset",           offset),
            ],
            timeout=120,
        )

        if resp.status_code >= 400:
            print(f"\nError {resp.status_code}: {resp.text}")
            resp.raise_for_status()

        rows = resp.json()
        all_rows.extend(rows)
        print(f"{len(rows)} rows  (total {len(all_rows):,})")

        if len(rows) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(0.05)

    return all_rows


def build_wide_dataframe(rows):
    df = pd.DataFrame(rows)
    df["reading_datetime"] = pd.to_datetime(df["reading_datetime"], utc=True)
    df["reading_datetime"] = df["reading_datetime"].dt.tz_convert("Asia/Singapore").dt.tz_localize(None)
    df["reading_value"]    = pd.to_numeric(df["reading_value"], errors="coerce")
    df["minute"]           = df["reading_datetime"].dt.floor("min")
    df["location_name"]    = df["location_id"].map(LOCATION_ID_TO_NAME).fillna(df["location_id"])

    wide = df.pivot_table(
        index="minute",
        columns="location_name",
        values="reading_value",
        aggfunc="mean",
    ).reset_index()
    wide.columns.name = None

    ordered = [v for v in LOCATION_ID_TO_NAME.values() if v in wide.columns]
    extra   = [c for c in wide.columns if c not in ordered and c != "minute"]
    wide    = wide[["minute"] + ordered + extra]

    wide.insert(0, "Date",      wide["minute"].dt.strftime("%d %b %y").str.upper())
    wide.insert(1, "Time",      wide["minute"].dt.strftime("%H:%M"))
    wide["Tab_Group"] = wide["minute"].dt.strftime("%Y-%m")
    wide = wide.drop(columns=["minute"])
    return wide


def write_excel(df):
    print(f"\nWriting Excel: {len(df):,} rows x {len(df.columns) - 1} cols into monthly tabs")

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

        for tab_group, group in df.groupby("Tab_Group"):
            clean = group.drop(columns=["Tab_Group"])
            tab   = pd.to_datetime(tab_group + "-01").strftime("%b %Y")

            clean.to_excel(writer, sheet_name=tab, index=False, na_rep="")
            ws      = writer.sheets[tab]
            n_rows  = len(clean)
            n_cols  = len(clean.columns)

            ws.hide_gridlines(0)
            for ci, cname in enumerate(clean.columns):
                ws.write(0, ci, cname, header_fmt)
            ws.set_row(0, 26)
            ws.set_column(0, 1, 14, center_fmt)
            ws.set_column(2, n_cols - 1, 20, numeric_fmt)
            ws.freeze_panes(1, 2)
            ws.autofilter(0, 0, n_rows, n_cols - 1)

            for ci, cname in enumerate(clean.columns):
                max_len = clean[cname].astype(str).head(500).str.len().max()
                ws.set_column(ci, ci, max(max_len + 4, 14))

    print(f"Saved: {OUTPUT_FILE}")


def main():
    print(f"Exporting {DATE_FROM[:10]} -> {DATE_TO[:10]}")
    print("Fetching raw rows from meter_readings (pivot happens in Python)...")
    rows = fetch_all_rows()
    if not rows:
        raise RuntimeError("No rows returned.")
    print(f"\nTotal raw rows: {len(rows):,}")
    print("Pivoting...")
    df = build_wide_dataframe(rows)
    write_excel(df)


if __name__ == "__main__":
    main()
