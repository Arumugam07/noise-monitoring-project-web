import psycopg2
import pandas as pd

AIVEN_DATABASE_URL = "YOUR_AIVEN_DATABASE_URL"

query = """
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
    FROM meter_readings

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
        ) AS "Singapore Sports School",

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 120 Serangoon North Ave 1'
        ) AS "BLK 120 Serangoon North Ave 1",

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 838 Hougang Central'
        ) AS "BLK 838 Hougang Central",

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 558 Jurong West Street 42'
        ) AS "BLK 558 Jurong West Street 42",

        MAX(reading_value) FILTER (
            WHERE location_name = 'Jurong Safra, Block C'
        ) AS "Jurong Safra, Block C",

        MAX(reading_value) FILTER (
            WHERE location_name = 'AMA KENG SITE'
        ) AS "AMA KENG SITE",

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 19 Balam Road'
        ) AS "BLK 19 Balam Road",

        MAX(reading_value) FILTER (
            WHERE location_name = 'Norcom II Tower 4'
        ) AS "Norcom II Tower 4",

        MAX(reading_value) FILTER (
            WHERE location_name = 'Blk 444 Choa Chu Kang Avenue 4'
        ) AS "Blk 444 Choa Chu Kang Avenue 4",

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 654B Punggol Drive'
        ) AS "BLK 654B Punggol Drive",

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 132B Tengah Garden Avenue'
        ) AS "BLK 132B Tengah Garden Avenue",

        MAX(reading_value) FILTER (
            WHERE location_name = 'BLK 206A Punggol Place'
        ) AS "BLK 206A Punggol Place",

        MAX(reading_value) FILTER (
            WHERE location_name = 'Woodlands 11'
        ) AS "Woodlands 11"

    FROM readings
    GROUP BY local_minute
)

SELECT
    TO_CHAR(m.local_minute, 'DD MON YY') AS "Date",
    TO_CHAR(m.local_minute, 'HH24:MI') AS "Time",

    ROUND(p."Singapore Sports School"::numeric, 1)
        AS "Singapore Sports School",

    ROUND(p."BLK 120 Serangoon North Ave 1"::numeric, 1)
        AS "BLK 120 Serangoon North Ave 1",

    ROUND(p."BLK 838 Hougang Central"::numeric, 1)
        AS "BLK 838 Hougang Central",

    ROUND(p."BLK 558 Jurong West Street 42"::numeric, 1)
        AS "BLK 558 Jurong West Street 42",

    ROUND(p."Jurong Safra, Block C"::numeric, 1)
        AS "Jurong Safra, Block C",

    ROUND(p."AMA KENG SITE"::numeric, 1)
        AS "AMA KENG SITE",

    ROUND(p."BLK 19 Balam Road"::numeric, 1)
        AS "BLK 19 Balam Road",

    ROUND(p."Norcom II Tower 4"::numeric, 1)
        AS "Norcom II Tower 4",

    ROUND(p."Blk 444 Choa Chu Kang Avenue 4"::numeric, 1)
        AS "Blk 444 Choa Chu Kang Avenue 4",

    ROUND(p."BLK 654B Punggol Drive"::numeric, 1)
        AS "BLK 654B Punggol Drive",

    ROUND(p."BLK 132B Tengah Garden Avenue"::numeric, 1)
        AS "BLK 132B Tengah Garden Avenue",

    ROUND(p."BLK 206A Punggol Place"::numeric, 1)
        AS "BLK 206A Punggol Place",

    ROUND(p."Woodlands 11"::numeric, 1)
        AS "Woodlands 11"

FROM minutes m

LEFT JOIN pivoted p
    ON p.local_minute = m.local_minute

ORDER BY m.local_minute;
"""

conn = psycopg2.connect(AIVEN_DATABASE_URL)

df = pd.read_sql(query, conn)

df.to_csv("Oct_2026.csv", index=False)

print("Rows exported:", len(df))

conn.close()
