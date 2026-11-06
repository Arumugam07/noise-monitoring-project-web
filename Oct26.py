import csv

from database import connection


OUTPUT_FILE = "Oct_2026.csv"


QUERY = """
WITH minutes AS (
    SELECT generate_series(
        TIMESTAMP '2026-10-01 00:00:00',
        TIMESTAMP '2026-10-31 23:59:00',
        INTERVAL '1 minute'
    ) AS local_minute
),

readings AS (
    SELECT
        DATE_TRUNC(
            'minute',
            reading_datetime AT TIME ZONE 'Asia/Singapore'
        ) AS local_minute,

        CASE
            WHEN location_name ILIKE '%Tengah Garden%'
                THEN 'BLK 132B Tengah Garden Avenue'
            ELSE location_name
        END AS location_name,

        reading_value

    FROM public.meter_readings

    WHERE reading_datetime >=
        TIMESTAMP '2026-10-01 00:00:00'
        AT TIME ZONE 'Asia/Singapore'

      AND reading_datetime <
        TIMESTAMP '2026-11-01 00:00:00'
        AT TIME ZONE 'Asia/Singapore'
),

pivoted AS (
    SELECT
        local_minute,

        MAX(reading_value) FILTER (
            WHERE location_name = 'Singapore Sports School'
        ) AS singapore_sports_school,

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 120 Serangoon North Ave 1'
        ) AS serangoon_north,

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 838 Hougang Central'
        ) AS hougang_central,

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 558 Jurong West Street 42'
        ) AS jurong_west,

        MAX(reading_value) FILTER (
            WHERE location_name = 'Jurong Safra, Block C'
        ) AS jurong_safra,

        MAX(reading_value) FILTER (
            WHERE location_name = 'AMA KENG SITE'
        ) AS ama_keng,

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 19 Balam Road'
        ) AS balam_road,

        MAX(reading_value) FILTER (
            WHERE location_name = 'Norcom II Tower 4'
        ) AS norcom,

        MAX(reading_value) FILTER (
            WHERE location_name = 'Blk 444 Choa Chu Kang Avenue 4'
        ) AS choa_chu_kang,

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 654B Punggol Drive'
        ) AS punggol_drive,

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 132B Tengah Garden Avenue'
        ) AS tengah,

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 206A Punggol Place'
        ) AS punggol_place,

        MAX(reading_value) FILTER (
            WHERE location_name = 'Woodlands 11'
        ) AS woodlands

    FROM readings

    GROUP BY local_minute
)

SELECT
    TO_CHAR(m.local_minute, 'DD MON YY') AS "Date",
    TO_CHAR(m.local_minute, 'HH24:MI') AS "Time",

    ROUND(p.singapore_sports_school::numeric, 1)
        AS "Singapore Sports School",

    ROUND(p.serangoon_north::numeric, 1)
        AS "BLK 120 Serangoon North Ave 1",

    ROUND(p.hougang_central::numeric, 1)
        AS "BLK 838 Hougang Central",

    ROUND(p.jurong_west::numeric, 1)
        AS "BLK 558 Jurong West Street 42",

    ROUND(p.jurong_safra::numeric, 1)
        AS "Jurong Safra, Block C",

    ROUND(p.ama_keng::numeric, 1)
        AS "AMA KENG SITE",

    ROUND(p.balam_road::numeric, 1)
        AS "BLK 19 Balam Road",

    ROUND(p.norcom::numeric, 1)
        AS "Norcom II Tower 4",

    ROUND(p.choa_chu_kang::numeric, 1)
        AS "Blk 444 Choa Chu Kang Avenue 4",

    ROUND(p.punggol_drive::numeric, 1)
        AS "BLK 654B Punggol Drive",

    ROUND(p.tengah::numeric, 1)
        AS "BLK 132B Tengah Garden Avenue",

    ROUND(p.punggol_place::numeric, 1)
        AS "BLK 206A Punggol Place",

    ROUND(p.woodlands::numeric, 1)
        AS "Woodlands 11"

FROM minutes m

LEFT JOIN pivoted p
    ON p.local_minute = m.local_minute

ORDER BY m.local_minute;
"""


def export_october():
    with connection() as conn:

        # Named cursor streams rows instead of loading everything at once.
        with conn.cursor(name="october_export") as cur:
            cur.itersize = 5000

            cur.execute(QUERY)

            headers = [column.name for column in cur.description]

            row_count = 0

            with open(
                OUTPUT_FILE,
                "w",
                newline="",
                encoding="utf-8-sig"
            ) as file:

                writer = csv.writer(file)

                # Excel column headers
                writer.writerow(headers)

                for row in cur:
                    writer.writerow(row)
                    row_count += 1

    print(f"Export completed: {OUTPUT_FILE}")
    print(f"Rows exported: {row_count}")

    if row_count == 44640:
        print("SUCCESS: Full October 2026 dataset exported.")
    else:
        print(
            f"WARNING: Expected 44,640 rows but received {row_count:,}."
        )


if __name__ == "__main__":
    export_october()
