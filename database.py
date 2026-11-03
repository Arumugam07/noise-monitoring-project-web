"""Small PostgreSQL data-access layer shared by the dashboard and ETL.

The existing table/filter call shape is retained, but every query executes
directly through psycopg2. No Supabase API or API key is used.
"""
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
import os
from pathlib import Path
import tempfile

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor, execute_values

WRITE_LOCK = 714021
REFRESH_LOCK = 714022
TABLES = {
    "meter_readings", "meter_readings_backup_before_timezone_fix",
    "monthly_persisted_summary", "noise_excel_export_2025_2026_v2",
    "wide_view", "wide_view_mv", "wide_view_mv_backup",
    "latest_monthly_summary", "monthly_chart_data",
    "noise_excel_export_2025_2026",
}
WRITABLE = {"meter_readings", "monthly_persisted_summary"}


def setting(name, default=None):
    value = os.getenv(name)
    if value:
        return value
    # Streamlit secrets do not automatically become environment variables.
    try:
        import streamlit as st
        return st.secrets.get(name, default)
    except (ImportError, FileNotFoundError):
        return default


@contextmanager
def connection(url=None, certificate=None, *, autocommit=False):
    url = url or setting("AIVEN_DATABASE_URL")
    certificate = certificate or setting("AIVEN_CA_CERT")
    if not url or not certificate:
        raise RuntimeError("Set AIVEN_DATABASE_URL and AIVEN_CA_CERT in environment or Streamlit secrets.")
    # Keyword SSL parameters override any weaker sslmode in the URI.
    with tempfile.TemporaryDirectory(prefix="noise-db-") as directory:
        cert = Path(directory) / "ca.pem"
        cert.write_text(certificate.strip() + "\n")
        cert.chmod(0o600)
        conn = psycopg2.connect(
            url, sslmode="verify-full", sslrootcert=str(cert),
            connect_timeout=30, application_name="noise-monitoring",
            options="-c timezone=UTC -c statement_timeout=2700000 -c lock_timeout=30000",
        )
        conn.autocommit = autocommit
        try:
            yield conn
            if not autocommit:
                conn.commit()
        except BaseException:
            if not autocommit:
                conn.rollback()
            raise
        finally:
            conn.close()


def json_value(value):
    # Preserve the data types the existing PostgREST consumers expect.
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


@dataclass
class Result:
    data: list
    count: int | None = None


class DatabaseClient:
    def __init__(self, url=None, certificate=None):
        self.url = url
        self.certificate = certificate

    def connect(self, **kwargs):
        return connection(self.url, self.certificate, **kwargs)

    def table(self, name):
        if name not in TABLES:
            raise ValueError(f"Unsupported application table: {name}")
        return Query(self, name)

    def refresh(self):
        with self.connect(autocommit=True) as conn, conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s)", (REFRESH_LOCK,))
            if not cur.fetchone()[0]:
                raise RuntimeError("Another dashboard refresh is already running.")
            try:
                cur.execute("SELECT ispopulated FROM pg_matviews WHERE schemaname='public' AND matviewname='wide_view_mv'")
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError("wide_view_mv has not been migrated.")
                command = "REFRESH MATERIALIZED VIEW "
                if row[0]:
                    command += "CONCURRENTLY "
                cur.execute(command + "public.wide_view_mv")
                cur.execute("ANALYZE public.wide_view_mv")
            finally:
                cur.execute("SELECT pg_advisory_unlock(%s)", (REFRESH_LOCK,))

    def date_range(self):
        with self.connect() as conn, conn.cursor() as cur:
            cur.execute('SELECT min("Date"), max("Date") FROM public.wide_view_mv')
            first, last = cur.fetchone()
        return Result([{"min_date": json_value(first), "max_date": json_value(last)}])


def create_client(url=None, certificate=None):
    """Construct a lightweight client; each executed query closes its connection."""
    return DatabaseClient(url, certificate)


class Query:
    def __init__(self, client, name):
        self.client, self.name = client, name
        self.columns = "*"
        self.exact_count = False
        self.filters, self.orders = [], []
        self.offset, self.size = 0, None
        self.records, self.conflict = None, None

    def select(self, columns="*", count=None):
        self.columns, self.exact_count = columns, count == "exact"
        return self

    def _filter(self, column, operator, value):
        self.filters.append((column, operator, value))
        return self

    def eq(self, column, value): return self._filter(column, "=", value)
    def gte(self, column, value): return self._filter(column, ">=", value)
    def lte(self, column, value): return self._filter(column, "<=", value)
    def lt(self, column, value): return self._filter(column, "<", value)

    def order(self, column, desc=False):
        self.orders.append((column, desc))
        return self

    def range(self, start, end):
        if start < 0 or end < start:
            raise ValueError("Invalid inclusive pagination range")
        self.offset, self.size = start, end - start + 1
        return self

    def limit(self, size):
        if size < 0:
            raise ValueError("Negative limit")
        self.size = size
        return self

    def upsert(self, records, on_conflict):
        if self.name not in WRITABLE:
            raise ValueError("Writes are restricted to readings and monthly summaries")
        self.records = records if isinstance(records, list) else [records]
        self.conflict = [c.strip() for c in on_conflict.split(",")]
        return self

    def execute(self):
        if self.records is not None:
            return self._write()
        params = []
        where = sql.SQL("")
        if self.filters:
            where = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(
                sql.SQL("{} {} %s").format(sql.Identifier(c), sql.SQL(op))
                for c, op, value in self.filters
            )
            params = [value for c, op, value in self.filters]
        fields = sql.SQL("*") if self.columns == "*" else sql.SQL(", ").join(
            sql.Identifier(c.strip()) for c in self.columns.split(",")
        )
        table = sql.Identifier("public", self.name)
        query = sql.SQL("SELECT {} FROM {}").format(fields, table) + where
        orders = self.orders or (
            [("reading_datetime", False), ("location_id", False)]
            if self.name == "meter_readings" else
            [("Date", False), ("Time", False)]
            if self.name in {"wide_view", "wide_view_mv", "wide_view_mv_backup"} else []
        )
        if orders:
            query += sql.SQL(" ORDER BY ") + sql.SQL(", ").join(
                sql.Identifier(c) + sql.SQL(" DESC" if desc else " ASC") for c, desc in orders
            )
        page_params = list(params)
        if self.size is not None:
            query += sql.SQL(" LIMIT %s OFFSET %s")
            page_params.extend([self.size, self.offset])
        with self.client.connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            count = None
            if self.exact_count:
                cur.execute(sql.SQL("SELECT count(*) AS total FROM {} ").format(table) + where, params)
                count = cur.fetchone()["total"]
            cur.execute(query, page_params)
            rows = [{k: json_value(v) for k, v in row.items()} for row in cur.fetchall()]
        return Result(rows, count)

    def _write(self):
        if not self.records:
            return Result([])
        columns = list(self.records[0])
        if any(set(r) != set(columns) for r in self.records):
            raise ValueError("All upsert records must contain the same columns")
        if not set(self.conflict) <= set(columns):
            raise ValueError("Conflict key is missing from records")
        # Deduplicate an entire batch before issuing INSERT, including page boundaries.
        records = list({tuple(r[c] for c in self.conflict): r for r in self.records}.values())
        updates = [c for c in columns if c not in self.conflict and c != "id"]
        assignments = sql.SQL(", ").join(
            sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in updates
        )
        action = sql.SQL("DO UPDATE SET ") + assignments if updates else sql.SQL("DO NOTHING")
        query = sql.SQL("INSERT INTO {} ({}) VALUES %s ON CONFLICT ({}) {} RETURNING *").format(
            sql.Identifier("public", self.name),
            sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(map(sql.Identifier, self.conflict)), action,
        )
        values = [tuple(r[c] for c in columns) for r in records]
        with self.client.connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (WRITE_LOCK,))
            rows = execute_values(cur, query, values, page_size=1000, fetch=True)
        return Result([{k: json_value(v) for k, v in row.items()} for row in rows])
