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

VIEW_NAME = "noise_excel_export_2025_2026_v2"
OUTPUT_FILE = "noise_2_jun_2025_to_2_jun_2026_v2.xlsx"
PAGE_SIZE = 1000


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
                "order": "minute.asc",
                "limit": PAGE_SIZE,
                "offset": offset,
            },
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
    rows = fetch_all_rows()

    if not rows:
        raise RuntimeError("No rows found.")

    df = pd.DataFrame(rows)
    df["minute"] = pd.to_datetime(df["minute"], errors="coerce")

    if df["minute"].dt.tz is not None:
        df["minute"] = (
            df["minute"]
            .dt.tz_convert("Asia/Singapore")
            .dt.tz_localize(None)
        )

    df = df.sort_values("minute")

    # --- 1. SEPARATE DATE & TIME AND CONVERT DATE TO UPPERCASE '17 NOV 25' ---
    df.insert(0, "Date", df["minute"].dt.strftime("%d %b %y").str.upper())
    df.insert(1, "Time", df["minute"].dt.strftime("%H:%M"))
    
    # Create an invisible helper string key for monthly tab grouping (e.g., "2025-11")
    df["Tab_Group"] = df["minute"].dt.strftime("%Y-%m")
    
    # Drop the old combined minute column
    df = df.drop(columns=["minute"])

    print(f"Writing Excel: {df.shape[0]} rows x {df.shape[1] - 1} columns into monthly tabs")

    # --- 2. MULTI-TAB MONTHLY SPLIT & EXECUTIVE STYLING ---
    with pd.ExcelWriter(OUTPUT_FILE, engine="xlsxwriter") as writer:
        workbook = writer.book

        # Define custom professional styles
        header_format = workbook.add_format({
            "font_name": "Segoe UI",
            "font_size": 11,
            "bold": True,
            "font_color": "#FFFFFF",
            "bg_color": "#1F4E78",     # Slate Navy
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "border_color": "#D9D9D9"
        })

        center_text_format = workbook.add_format({
            "font_name": "Segoe UI",
            "font_size": 10,
            "align": "center",
            "valign": "vcenter",
            "border": 1,
            "border_color": "#D9D9D9"
        })

        numeric_format = workbook.add_format({
            "font_name": "Segoe UI",
            "font_size": 10,
            "num_format": "0.00",       # Forces 2 decimals
            "align": "right",
            "valign": "vcenter",
            "border": 1,
            "border_color": "#D9D9D9"
        })

        # Group data by the month helper and generate tabs
        for tab_group, group in df.groupby("Tab_Group"):
            # Drop the helper column so it doesn't print to Excel
            clean_group = group.drop(columns=["Tab_Group"])
            
            # Convert "2025-11" to a friendly tab title like "Nov 2025"
            tab_name = pd.to_datetime(tab_group + "-01").strftime("%b %Y")
            
            # Write out this month's data block
            clean_group.to_excel(writer, sheet_name=tab_name, index=False, na_rep="")
            worksheet = writer.sheets[tab_name]

            # Enforce native grid lines visibility
            worksheet.hide_gridlines(0)

            num_rows = len(clean_group)
            num_cols = len(clean_group.columns)

            # Apply custom formatted headers manually over pandas defaults
            for col_idx, col_name in enumerate(clean_group.columns):
                worksheet.write(0, col_idx, col_name, header_format)
            worksheet.set_row(0, 26)  # Generous header height padding

            # Style the column groups (Data, Time, and Decimals)
            worksheet.set_column(0, 1, 14, center_text_format)         # Center Date & Time
            worksheet.set_column(2, num_cols - 1, 20, numeric_format)   # Right-align & pad sensor numbers

            # Add modern UI elements (Freeze top header, freeze Date/Time cols, enable filters)
            worksheet.freeze_panes(1, 2)
            worksheet.autofilter(0, 0, num_rows, num_cols - 1)

            # Dynamic Column Auto-Fit Engine
            for col_idx, col_name in enumerate(clean_group.columns):
                # Sample max text length up to 500 rows for speedy code performance
                max_len = clean_group[col_name].astype(str).head(500).str.len().max()
                max_len = max(max_len, len(col_name)) + 4
                # Maintain safe visual boundaries
                worksheet.set_column(col_idx, col_idx, max(max_len, 14))

    print(f"Saved beautifully styled monthly tabs to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
