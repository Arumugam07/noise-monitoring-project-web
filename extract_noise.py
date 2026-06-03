import os
import time
import requests
import pandas as pd


SUPABASE_URL = os.environ.get(
    "SUPABASE_URL",
    "https://tgznxzfdlxhxqwpohhyl.supabase.co",
)
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

VIEW_NAME = "noise_excel_export_2025_2026"
OUTPUT_FILE = "noise_2_jun_2025_to_2_jun_2026.xlsx"

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

        params = {
            "select": "*",
            "order": "minute.asc",
            "limit": str(PAGE_SIZE),
            "offset": str(offset),
        }

        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/{VIEW_NAME}",
            headers=headers,
            params=params,
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
        df["minute"] = df["minute"].dt.tz_convert("Asia/Singapore").dt.tz_localize(None)

    df = df.sort_values("minute")

    print(f"Writing Excel: {df.shape[0]} rows x {df.shape[1]} columns")

    with pd.ExcelWriter(
        OUTPUT_FILE,
        engine="xlsxwriter",
        datetime_format="d mmm yy hh:mm",
    ) as writer:
        df.to_excel(writer, sheet_name="Noise Data", index=False)

        workbook = writer.book
        worksheet = writer.sheets["Noise Data"]

        date_format = workbook.add_format({"num_format": "d mmm yy hh:mm"})
        worksheet.freeze_panes(1, 1)
        worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
        worksheet.set_column(0, 0, 18, date_format)
        worksheet.set_column(1, len(df.columns) - 1, 16)

    print(f"Saved {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
