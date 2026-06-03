import os
from datetime import datetime, timezone

import pandas as pd
import psycopg2


DATABASE_URL = os.environ["DATABASE_URL"]

YEAR = int(os.environ.get("YEAR", "2025"))
OUTPUT_FILE = f"decibel_data_{YEAR}.xlsx"

TABLE = "public.meter_readings"
TIME_COL = "reading_datetime"
LOCATION_COL = "location_name"
DECIBEL_COL = "reading_value"


def month_bounds(year, month):
    start = datetime(year, month, 1, tzinfo=timezone.utc)

    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)

    return start, end


def fetch_month(conn, start, end):
    query = f"""
        select
            {TIME_COL},
            {LOCATION_COL},
            {DECIBEL_COL}
        from {TABLE}
        where {TIME_COL} >= %s
          and {TIME_COL} < %s
        order by {TIME_COL}, {LOCATION_COL}
    """

    return pd.read_sql_query(query, conn, params=(start, end))


def write_month(writer, df, sheet_name):
    if df.empty:
        return 0, 0

    df[TIME_COL] = pd.to_datetime(df[TIME_COL], errors="coerce")
    df[DECIBEL_COL] = pd.to_numeric(df[DECIBEL_COL], errors="coerce")
    df = df.dropna(subset=[TIME_COL, LOCATION_COL, DECIBEL_COL])

    df["minute"] = df[TIME_COL].dt.floor("min")

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
    print(f"Exporting {YEAR} from {TABLE}")
    summary_rows = []

    conn = psycopg2.connect(DATABASE_URL)

    try:
        with pd.ExcelWriter(
            OUTPUT_FILE,
            engine="xlsxwriter",
            datetime_format="d mmm yy hh:mm",
        ) as writer:
            for month in range(1, 13):
                start, end = month_bounds(YEAR, month)
                sheet_name = start.strftime("%b %Y")

                print(f"Fetching {sheet_name}...")
                df = fetch_month(conn, start, end)

                row_count, location_count = write_month(writer, df, sheet_name)

                summary_rows.append(
                    {
                        "month": sheet_name,
                        "rows": row_count,
                        "locations": location_count,
                    }
                )

                print(f"Wrote {row_count} rows across {location_count} locations.")

            summary = pd.DataFrame(summary_rows)
            summary.to_excel(writer, sheet_name="Summary", index=False)
            writer.sheets["Summary"].set_column(0, 0, 14)
            writer.sheets["Summary"].set_column(1, 2, 12)

    finally:
        conn.close()

    print(f"Saved {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
