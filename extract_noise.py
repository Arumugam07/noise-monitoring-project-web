import os
import time
import requests
import pandas as pd
from xlsxwriter.utility import xl_col_to_name

SUPABASE_URL = os.environ.get(
    "SUPABASE_URL",
    "https://tgznxzfdlxhxqwpohhyl.supabase.co",
)
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

VIEW_NAME = "noise_excel_export_2025_2026"
OUTPUT_FILE = "noise_1_apr_2025_to_31_may_2026.xlsx"
PAGE_SIZE = 1000

# Inclusive date range (SGT — the view stores timestamps in SGT)
DATE_FROM = "2025-04-01T00:00:00"
DATE_TO   = "2026-05-31T23:59:59"

def fetch_all_rows():
    all_rows = []
    offset = 0

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

    while True:
        print(f"Fetching rows {offset} to {offset + PAGE_SIZE - 1}...")

        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/{VIEW_NAME}",
            headers=headers,
            params={
                "select": "*",
                "minute": f"gte.{DATE_FROM}",
                "minute": f"lte.{DATE_TO}",   # overrides above — use explicit params below
                "order":  "minute.asc",
                "limit":  PAGE_SIZE,
                "offset": offset,
            },
            timeout=120,
        )

        # Supabase needs range filters as separate params with the same key;
        # requests deduplicates dict keys, so pass them as a list of tuples.
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/{VIEW_NAME}",
            headers=headers,
            params=[
                ("select", "*"),
                ("minute", f"gte.{DATE_FROM}"),
                ("minute", f"lte.{DATE_TO}"),
                ("order",  "minute.asc"),
                ("limit",  PAGE_SIZE),
                ("offset", offset),
            ],
            timeout=120,
            
        )

        if response.status_code >= 400:
            print(response.text)
            response.raise_for_status()

        rows = response.json()
        all_rows.extend(rows)

        print(f"Fetched {len(rows)} rows. Total so far: {len(all_rows)}")

        if len(rows) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(0.1)

    return all_rows


def main():
    print(f"Exporting {DATE_FROM[:10]} → {DATE_TO[:10]}")
    rows = fetch_all_rows()
 
    if not rows:
        raise RuntimeError("No rows returned — check view name and date range.")
 
    df = pd.DataFrame(rows)
    df["minute"] = pd.to_datetime(df["minute"], errors="coerce")
 
    # Convert UTC → SGT if timezone-aware
    if df["minute"].dt.tz is not None:
        df["minute"] = (
            df["minute"]
            .dt.tz_convert("Asia/Singapore")
            .dt.tz_localize(None)
        )
 
    df = df.sort_values("minute")
 
    # Hard-clip to the exact requested window after tz conversion
    df = df[
        (df["minute"] >= pd.Timestamp(DATE_FROM)) &
        (df["minute"] <= pd.Timestamp(DATE_TO))
    ]
 
    print(f"Rows after date clip: {len(df):,}")
 
    # Separate Date / Time columns
    df.insert(0, "Date", df["minute"].dt.strftime("%d %b %y").str.upper())
    df.insert(1, "Time", df["minute"].dt.strftime("%H:%M"))
    df["Tab_Group"] = df["minute"].dt.strftime("%Y-%m")
    df = df.drop(columns=["minute"])
 
    print(f"Writing Excel: {df.shape[0]:,} rows × {df.shape[1] - 1} cols into monthly tabs")
 
    with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
        workbook = writer.book
 
        header_format = workbook.add_format({
            "font_name":   "Segoe UI",
            "font_size":   11,
            "bold":        True,
            "font_color":  "#FFFFFF",
            "bg_color":    "#1F4E78",
            "align":       "center",
            "valign":      "vcenter",
            "border":      1,
            "border_color":"#D9D9D9",
        })
 
        center_text_format = workbook.add_format({
            "font_name":   "Segoe UI",
            "font_size":   10,
            "align":       "center",
            "valign":      "vcenter",
            "border":      1,
            "border_color":"#D9D9D9",
        })
 
        numeric_format = workbook.add_format({
            "font_name":   "Segoe UI",
            "font_size":   10,
            "num_format":  "0.00",
            "align":       "right",
            "valign":      "vcenter",
            "border":      1,
            "border_color":"#D9D9D9",
        })
 
        for tab_group, group in df.groupby("Tab_Group"):
            clean_group = group.drop(columns=["Tab_Group"])
            tab_name    = pd.to_datetime(tab_group + "-01").strftime("%b %Y")
 
            clean_group.to_excel(writer, sheet_name=tab_name, index=False, na_rep="")
            worksheet = writer.sheets[tab_name]
            worksheet.hide_gridlines(0)
 
            num_rows = len(clean_group)
            num_cols = len(clean_group.columns)
 
            for col_idx, col_name in enumerate(clean_group.columns):
                worksheet.write(0, col_idx, col_name, header_format)
            worksheet.set_row(0, 26)
 
            worksheet.set_column(0, 1, 14, center_text_format)
            worksheet.set_column(2, num_cols - 1, 20, numeric_format)
 
            worksheet.freeze_panes(1, 2)
            worksheet.autofilter(0, 0, num_rows, num_cols - 1)
 
            for col_idx, col_name in enumerate(clean_group.columns):
                max_len = clean_group[col_name].astype(str).head(500).str.len().max()
                max_len = max(max_len, len(col_name)) + 4
                worksheet.set_column(col_idx, col_idx, max(max_len, 14))
 
    print(f"Saved: {OUTPUT_FILE}")
 
 
if __name__ == "__main__":
    main()
