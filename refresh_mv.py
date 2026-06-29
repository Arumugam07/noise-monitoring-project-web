#!/usr/bin/env python3
"""
Refresh wide_view_mv after daily ETL completes.
Uses Supabase RPC so no direct DB credentials needed.
Falls back to SUPABASE_SERVICE_KEY for elevated permissions if needed.
"""
import os
import logging
from dotenv import load_dotenv
from supabase import create_client

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("refresh-mv")


def main():
    load_dotenv()

    try:
        supabase_url = os.environ["SUPABASE_URL"]
        supabase_key = os.getenv("SUPABASE_SERVICE_KEY") or os.environ["SUPABASE_ANON_KEY"]
    except KeyError as e:
        log.error(f"Missing environment variable: {e}")
        raise SystemExit(1)

    supabase = create_client(supabase_url, supabase_key)

    log.info("=" * 60)
    log.info("REFRESHING wide_view_mv via RPC")
    log.info("=" * 60)

    try:
        # Calls the existing public.refresh_wide_view_mv() function in Supabase
        # This is the non-CONCURRENT version — safe to call anytime,
        # and guaranteed to work even if the unique index is missing.
        result = supabase.rpc("refresh_wide_view_mv").execute()
        log.info("✅ MV refresh triggered successfully via RPC")
        log.info(f"   Response: {result.data}")

    except Exception as e:
        log.warning(f"RPC call failed: {e}")
        log.warning("Attempting direct SQL fallback via PostgREST...")

        # Fallback: call via raw SQL through the REST API
        try:
            result = supabase.rpc(
                "exec_sql",
                {"query": "REFRESH MATERIALIZED VIEW public.wide_view_mv;"}
            ).execute()
            log.info("✅ MV refresh via fallback SQL succeeded")
        except Exception as e2:
            log.error(f"Both refresh methods failed: {e2}")
            log.error("Manual intervention required: run REFRESH MATERIALIZED VIEW public.wide_view_mv; in SQL Editor")
            raise SystemExit(1)

    # Verify the MV is current
    try:
        result = supabase.table("wide_view_mv") \
            .select("Date") \
            .order("Date", desc=True) \
            .limit(1) \
            .execute()

        if result.data:
            latest_date = result.data[0]["Date"]
            log.info(f"✅ MV latest date after refresh: {latest_date}")
        else:
            log.warning("MV appears empty after refresh — check Supabase logs")
    except Exception as e:
        log.warning(f"Could not verify MV date: {e}")

    log.info("=" * 60)
    log.info("REFRESH COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
