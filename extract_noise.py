import os
import time
import requests
import pandas as pd


SUPABASE_URL = "https://tgznxzfdlxhxqwpohhyl.supabase.co"
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

OUTPUT_FILE = "noise_2_jun_2025_to_2_jun_2026.xlsx"

TABLE = "meter_readings"
START_DATE = "2025-06-02T00:00:00+08:00"
END_DATE = "2026-06-03T00:00:00+08:00"

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
            "select": "reading_datetime,location_id,location_name,reading_value",
            "reading_datetime": [
                f"gte.{START_DATE}",
                f"lt.{END_DATE}",
            ],
            "order": "reading_datetime.asc,location_name.asc",
            "limit": str(PAGE_SIZE),
            "offset": str(offset),
        }

        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/{TABLE}",
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
        time.sleep(0.2)

    return all_rows


def main():
    rows = fetch_all_rows()

    if not rows:
        raise RuntimeError("No rows found for this date range.")

    df = pd.DataFrame(rows)

    df["reading_datetime"] = pd.to_datetime(df["reading_datetime"], errors="coerce")
    df["reading_value"] = pd.to_numeric(df["reading_value"], errors="coerce")
    df = df.dropna(subset=["reading_datetime", "location_name", "reading_value"])

    # Convert to Singapore time, then remove timezone because Excel cannot write timezone-aware datetimes.
    df["reading_datetime"] = (
        df["reading_datetime"]
        .dt.tz_convert("Asia/Singapore")
        .dt.tz_localize(None)
    )

    df["minute"] = df["reading_datetime"].dt.floor("min")

    pivot = (
        df.pivot_table(
            index="minute",
            columns="location_name",
            values="reading_value",
            aggfunc="mean",
        )
        .sort_index()
        .reset_index()
    )

    print(f"Writing Excel: {pivot.shape[0]} rows x {pivot.shape[1]} columns")

    with pd.ExcelWriter(
        OUTPUT_FILE,
        engine="xlsxwriter",
        datetime_format="d mmm yy hh:mm",
    ) as writer:
        pivot.to_excel(writer, sheet_name="Noise Data", index=False)

        workbook = writer.book
        worksheet = writer.sheets["Noise Data"]

        date_format = workbook.add_format({"num_format": "d mmm yy hh:mm"})
        worksheet.freeze_panes(1, 1)
        worksheet.autofilter(0, 0, len(pivot), len(pivot.columns) - 1)
        worksheet.set_column(0, 0, 18, date_format)
        worksheet.set_column(1, len(pivot.columns) - 1, 14)

    print(f"Saved {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
