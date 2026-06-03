import os
import pandas as pd
from supabase import create_client
from datetime import datetime, timezone

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

TABLE = "your_table_name"          # change this
TIME_COL = "recorded_at"           # change this
LOCATION_COL = "location"          # change this
DECIBEL_COL = "decibel"            # change this

YEAR = 2025
PAGE_SIZE = 1000

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_month(start, end):
    rows = []
    offset = 0

    while True:
        result = (
            supabase.table(TABLE)
            .select(f"{LOCATION_COL},{TIME_COL},{DECIBEL_COL}")
            .gte(TIME_COL, start)
            .lt(TIME_COL, end)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )

        batch = result.data or []
        rows.extend(batch)

        if len(batch) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    return rows

output_file = f"decibel_data_{YEAR}.xlsx"

with pd.ExcelWriter(output_file, engine="xlsxwriter", datetime_format="d mmm yy hh:mm") as writer:
    for month in range(1, 13):
        start = datetime(YEAR, month, 1, tzinfo=timezone.utc)

        if month == 12:
            end = datetime(YEAR + 1, 1, 1, tzinfo=timezone.utc)
        else:
            end = datetime(YEAR, month + 1, 1, tzinfo=timezone.utc)

        print(f"Fetching {start:%b %Y}...")

        rows = fetch_month(start.isoformat(), end.isoformat())

        if not rows:
            continue

        df = pd.DataFrame(rows)
        df[TIME_COL] = pd.to_datetime(df[TIME_COL])

        # Round/floor to minute in case readings include seconds.
        df["minute"] = df[TIME_COL].dt.floor("min")

        # If multiple readings exist per minute/location, average them.
        pivot = (
            df.pivot_table(
                index="minute",
                columns=LOCATION_COL,
                values=DECIBEL_COL,
                aggfunc="mean",
            )
            .sort_index()
            .reset_index()
        )

        sheet_name = start.strftime("%b %Y")
        pivot.to_excel(writer, sheet_name=sheet_name, index=False)

        workbook = writer.book
        worksheet = writer.sheets[sheet_name]

        date_format = workbook.add_format({"num_format": "d mmm yy hh:mm"})
        worksheet.set_column(0, 0, 18, date_format)
        worksheet.set_column(1, len(pivot.columns), 14)

print(f"Saved {output_file}")
