#!/usr/bin/env python3
"""
Refresh the wide_view_mv materialized view in Supabase.

Called by weekly_alert.yml BEFORE the health check runs,
so the health check always gets fresh data.

Requires the following SQL function to exist in Supabase (run once):

    CREATE OR REPLACE FUNCTION refresh_wide_view()
    RETURNS void
    LANGUAGE sql
    SECURITY DEFINER
    AS $$
      REFRESH MATERIALIZED VIEW public.wide_view_mv;
    $$;

Also requires SUPABASE_SERVICE_KEY (not anon key) as a GitHub secret,
since refreshing a materialized view needs elevated permissions.
"""

import os
import sys
import logging
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("refresh-mv")

SUPABASE_URL = os.getenv("SUPABASE_URL")
# Use service role key for MV refresh — anon key doesn't have permission
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")


def refresh_via_rpc():
    """Call the refresh_wide_view() RPC function in Supabase."""
    key = SUPABASE_SERVICE_KEY or SUPABASE_ANON_KEY

    if not SUPABASE_URL or not key:
        log.error("SUPABASE_URL and SUPABASE_SERVICE_KEY (or SUPABASE_ANON_KEY) must be set")
        sys.exit(1)

    url = f"{SUPABASE_URL}/rest/v1/rpc/refresh_wide_view"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json"
    }

    log.info("Calling refresh_wide_view() RPC...")
    resp = requests.post(url, headers=headers, json={}, timeout=120)

    if resp.status_code in (200, 204):
        log.info("✅ wide_view_mv refreshed successfully")
    else:
        log.error(f"❌ Refresh failed: HTTP {resp.status_code} — {resp.text}")
        # Exit with error so GitHub Actions marks the step as failed
        sys.exit(1)


def main():
    refresh_via_rpc()


if __name__ == "__main__":
    main()
