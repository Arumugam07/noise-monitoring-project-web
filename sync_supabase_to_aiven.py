#!/usr/bin/env python3
"""Copy a Singapore date window from Supabase to Aiven without re-fetching the API.

Source reads use a repeatable-read snapshot. Target writes are transactional,
rerunnable upserts; existing IDs are kept and no historical readings are deleted.
Every source row in the snapshot is compared with the target before commit.
"""
import argparse
from datetime import date, datetime, timedelta, timezone
import logging
import os
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from database import WRITE_LOCK, create_client

SGT = ZoneInfo("Asia/Singapore")
READING_COLUMNS = ["location_id", "location_name", "reading_value", "reading_datetime", "created_at"]
log = logging.getLogger("migration-sync")


def singapore_bounds(start, end):
    if start > end:
        raise ValueError("Start date must be on or before end date")
    first = datetime.combine(start, datetime.min.time(), SGT)
    last = datetime.combine(end + timedelta(days=1), datetime.min.time(), SGT)
    return first.astimezone(timezone.utc), last.astimezone(timezone.utc)


def copy_readings(source, target, start, end):
    first, last = singapore_bounds(start, end)
    columns = sql.SQL(", ").join(map(sql.Identifier, READING_COLUMNS))
    with target.cursor() as dst:
        dst.execute("SELECT pg_advisory_xact_lock(%s)", (WRITE_LOCK,))
        dst.execute(sql.SQL("CREATE TEMP TABLE migration_readings ON COMMIT DROP AS SELECT {} FROM public.meter_readings WITH NO DATA").format(columns))
        copied = 0
        with source.cursor(name="migration_readings_source") as src:
            src.execute(sql.SQL("SELECT {} FROM public.meter_readings WHERE reading_datetime >= %s AND reading_datetime < %s ORDER BY reading_datetime, location_id").format(columns), (first, last))
            while True:
                batch = src.fetchmany(5000)
                if not batch:
                    break
                execute_values(dst, sql.SQL("INSERT INTO migration_readings ({}) VALUES %s").format(columns), batch, page_size=1000)
                copied += len(batch)
        if not copied:
            raise RuntimeError("Source window contains no readings; check the dates before proceeding")
        dst.execute("CREATE UNIQUE INDEX ON migration_readings (location_id, reading_datetime)")
        dst.execute(sql.SQL("""
            INSERT INTO public.meter_readings AS existing ({columns})
            SELECT {columns} FROM migration_readings WHERE true
            ON CONFLICT (location_id, reading_datetime) DO UPDATE SET
                location_name = EXCLUDED.location_name,
                reading_value = EXCLUDED.reading_value,
                created_at = EXCLUDED.created_at
            WHERE (existing.location_name, existing.reading_value, existing.created_at)
                IS DISTINCT FROM (EXCLUDED.location_name, EXCLUDED.reading_value, EXCLUDED.created_at)
        """).format(columns=columns))
        changed = dst.rowcount
        dst.execute(sql.SQL("""
            SELECT count(*) FROM (
                SELECT {columns} FROM migration_readings
                EXCEPT ALL
                SELECT {columns} FROM public.meter_readings
                WHERE reading_datetime >= %s AND reading_datetime < %s
            ) AS mismatches
        """).format(columns=columns), (first, last))
        mismatches = dst.fetchone()[0]
        if mismatches:
            raise RuntimeError(f"Verification failed for {mismatches} readings; transaction will roll back")
        dst.execute("""
            SELECT (reading_datetime AT TIME ZONE 'Asia/Singapore')::date,
                   count(*), count(DISTINCT location_id)
            FROM migration_readings GROUP BY 1 ORDER BY 1
        """)
        for day, rows, sites in dst.fetchall():
            log.info("Verified source snapshot: %s | %s readings | %s location IDs", day, rows, sites)
        dst.execute("""
            SELECT count(*) FROM public.meter_readings t
            WHERE t.reading_datetime >= %s AND t.reading_datetime < %s
              AND NOT EXISTS (
                  SELECT 1 FROM migration_readings s
                  WHERE s.location_id=t.location_id AND s.reading_datetime=t.reading_datetime
              )
        """, (first, last))
        extras = dst.fetchone()[0]
    log.info("Verified %s source readings; inserted/updated %s; retained %s additional target readings", copied, changed, extras)
    return copied, changed


def copy_summaries(source, target):
    with source.cursor() as src, target.cursor() as dst:
        src.execute("SELECT * FROM public.monthly_persisted_summary")
        names = [c.name for c in src.description]
        keep = [i for i, name in enumerate(names) if name != "id"]
        columns = [names[i] for i in keep]
        rows = [tuple(row[i] for i in keep) for row in src.fetchall()]
        if not rows:
            return
        keys = {"location_id", "month", "min_db", "max_db", "duration_minutes"}
        updates = sql.SQL(", ").join(
            sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in columns if c not in keys
        )
        execute_values(dst, sql.SQL("INSERT INTO public.monthly_persisted_summary ({}) VALUES %s ON CONFLICT (location_id,month,min_db,max_db,duration_minutes) DO UPDATE SET {}").format(sql.SQL(", ").join(map(sql.Identifier, columns)), updates), rows)
        log.info("Copied %s monthly summary rows from the same source snapshot", len(rows))


def preserve_backup_snapshot(source, target):
    # A materialized view cannot accept COPY/INSERT. Preserve the exact stored
    # source snapshot as a clearly named ordinary archive table instead.
    with source.cursor() as src:
        src.execute("SELECT ispopulated FROM pg_matviews WHERE schemaname='public' AND matviewname='wide_view_mv_backup'")
        row = src.fetchone()
        if not row or not row[0]:
            log.info("No populated source backup materialized view to archive")
            return
    with target.cursor() as dst:
        dst.execute("SELECT to_regclass('public.wide_view_mv_backup_snapshot')")
        if dst.fetchone()[0]:
            log.info("Existing backup snapshot archive retained unchanged")
            return
        dst.execute("CREATE TABLE public.wide_view_mv_backup_snapshot AS SELECT * FROM public.wide_view_mv_backup WITH NO DATA")
        copied = 0
        with source.cursor(name="migration_backup_snapshot") as src:
            src.execute("SELECT * FROM public.wide_view_mv_backup")
            while True:
                rows = src.fetchmany(5000)
                if not rows:
                    break
                execute_values(dst, "INSERT INTO public.wide_view_mv_backup_snapshot VALUES %s", rows, page_size=1000)
                copied += len(rows)
        dst.execute("SELECT count(*) FROM public.wide_view_mv_backup_snapshot")
        if dst.fetchone()[0] != copied:
            raise RuntimeError("Backup archive row count mismatch")
        log.info("Archived %s exact backup-view rows in wide_view_mv_backup_snapshot", copied)


def main():
    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=date.fromisoformat, default=date(2026, 10, 20))
    parser.add_argument("--end", type=date.fromisoformat, default=datetime.now(SGT).date())
    args = parser.parse_args()
    singapore_bounds(args.start, args.end)
    source_url = os.environ["MIGRATION_SOURCE_DATABASE_URL"]
    source = psycopg2.connect(source_url, sslmode="require", connect_timeout=30,
                              options="-c timezone=UTC -c statement_timeout=2700000")
    source.set_session(readonly=True, isolation_level="REPEATABLE READ")
    try:
        client = create_client()
        with client.connect() as target:
            copy_readings(source, target, args.start, args.end)
            copy_summaries(source, target)
            preserve_backup_snapshot(source, target)
        log.info("Catch-up committed. Supabase was read only throughout.")
        client.refresh()
        log.info("Dashboard refreshed: %s", client.date_range().data)
    finally:
        source.close()


if __name__ == "__main__":
    main()
