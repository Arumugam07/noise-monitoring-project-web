import os
from datetime import datetime, timezone

import pandas as pd
from supabase import create_client


SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

YEAR = int(os.environ.get("YEAR", "2025"))
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "1000"))

OUTPUT_FILE = os.environ.get("OUTPUT_FILE", f"decibel_data_{YEAR}.xlsx")

TABLE = os.environ.get("NOISE_TABLE", "meter_readings")
TIME_COL = os.environ.get("TIME_COL", "reading_datetime")
LOCATION_COL = os.environ.get("LOCATION_COL", "location_name")
DECIBEL_COL = os.environ.get("DECIBEL_COL", "reading_value")


def fetch_month(supabase, table, time_col, location_col, decibel_col, start, end):
    rows = []
    offset = 0
    columns = f"{location_col},{time_col},{decibel_col}"

    while True:
        result = (
            supabase.table(table)
            .select(columns)
            .gte(time_col, start)
            .lt(time_col, end)
            .range(offset, offset + PAGE_SIZE - 1)
            .execute()
        )

        batch = result.data or []
        rows.extend(batch)

        if len(batch) < PAGE_SIZE:
            break

        offset += PAGE_SIZE

    return rows


def month_bounds(year, month):
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return start, end


def write_month_sheet(writer, rows, sheet_name, time_col, location_col, decibel_col):
    df = pd.DataFrame(rows)
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col, location_col, decibel_col])
    df["minute"] = df[time_col].dt.floor("min")

    pivot = (
        df.pivot_table(
            index="minute",
            columns=location_col,
            values=decibel_col,
            aggfunc="mean",
        )
        .sort_index()
        .reset_index()
    )

    pivot.to_excel(writer, sheet_name=sheet_name, index=False)

    workbook = writer.book
    worksheet = writer.sheets[sheet_name]
    date_format = workbook.add_format({"num_format": "d mmm yy hh:mm"})

    worksheet.freeze_panes(1, 1)
    worksheet.autofilter(0, 0, max(len(pivot), 1), max(len(pivot.columns) - 1, 0))
    worksheet.set_column(0, 0, 18, date_format)
    worksheet.set_column(1, max(len(pivot.columns) - 1, 1), 14)

    return len(df), max(len(pivot.columns) - 1, 0)


def main():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    print("Using Supabase mapping:")
    print(f"  table: {TABLE}")
    print(f"  time column: {TIME_COL}")
    print(f"  location column: {LOCATION_COL}")
    print(f"  decibel column: {DECIBEL_COL}")

    summary_rows = []

    with pd.ExcelWriter(
        OUTPUT_FILE,
        engine="xlsxwriter",
        datetime_format="d mmm yy hh:mm",
    ) as writer:
        for month in range(1, 13):
            start, end = month_bounds(YEAR, month)
            sheet_name = start.strftime("%b %Y")
            print(f"Fetching {sheet_name}...")

            rows = fetch_month(
                supabase,
                TABLE,
                TIME_COL,
                LOCATION_COL,
                DECIBEL_COL,
                start.isoformat(),
                end.isoformat(),
            )

            if not rows:
                print(f"  No rows for {sheet_name}.")
                summary_rows.append({"month": sheet_name, "rows": 0, "locations": 0})
                continue

            row_count, location_count = write_month_sheet(
                writer,
                rows,
                sheet_name,
                TIME_COL,
                LOCATION_COL,
                DECIBEL_COL,
            )
            summary_rows.append(
                {
                    "month": sheet_name,
                    "rows": row_count,
                    "locations": location_count,
                }
            )
            print(f"  Wrote {row_count} rows across {location_count} locations.")

        summary = pd.DataFrame(summary_rows)
        summary.to_excel(writer, sheet_name="Summary", index=False)
        worksheet = writer.sheets["Summary"]
        worksheet.set_column(0, 0, 14)
        worksheet.set_column(1, 2, 12)

    print(f"Saved {OUTPUT_FILE}")
