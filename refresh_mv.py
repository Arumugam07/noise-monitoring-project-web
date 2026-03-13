#!/usr/bin/env python3
"""
Refresh wide_view_mv via direct Postgres connection (bypasses PostgREST timeout).
"""

import os
import sys
import logging
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
log = logging.getLogger(__name__)

def main():
    db_url = os.getenv("DATABASE_URL")

    if not db_url:
        log.error("DATABASE_URL not set. Get it from Supabase → Settings → Database → Connection string (URI mode).")
        sys.exit(1)

    log.info("Connecting directly to Postgres...")

    try:
        # options=-c... sets statement_timeout=0 at connection level, bypassing PostgREST limits
        conn = psycopg2.connect(db_url, options="-c statement_timeout=0")
        conn.autocommit = True  # REFRESH MV cannot run inside a transaction

        with conn.cursor() as cur:
            log.info("Refreshing wide_view_mv...")
            cur.execute("REFRESH MATERIALIZED VIEW public.wide_view_mv;")
            log.info("✅ wide_view_mv refreshed successfully")

        conn.close()

    except Exception as e:
        log.error(f"❌ Refresh failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
