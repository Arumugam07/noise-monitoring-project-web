#!/usr/bin/env python3
"""
Refresh the wide_view_mv materialized view in Supabase.
Called after daily ETL to ensure the dashboard shows fresh data.
"""

import os
import sys
import logging
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("refresh-mv")


def main():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_ANON_KEY")

    if not url or not key:
        log.error("SUPABASE_URL and SUPABASE_ANON_KEY must be set")
        sys.exit(1)

    supabase = create_client(url, key)

    log.info("Refreshing wide_view_mv...")
    try:
        supabase.rpc("refresh_wide_view_mv").execute()
        log.info("✅ wide_view_mv refreshed successfully")
    except Exception as e:
        log.error(f"❌ Failed to refresh via RPC: {e}")
        log.info("Note: Make sure the SQL function exists in Supabase (see instructions below)")
        sys.exit(1)


if __name__ == "__main__":
    main()
