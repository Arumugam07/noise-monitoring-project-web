#!/usr/bin/env python3
import os, sys, logging, requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger(__name__)

def main():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_KEY")  # service key, not anon

    if not url or not key:
        log.error("SUPABASE_URL or SUPABASE_SERVICE_KEY not set.")
        sys.exit(1)

    log.info("Refreshing wide_view_mv via Supabase RPC...")
    response = requests.post(
        f"{url}/rest/v1/rpc/refresh_wide_view_mv",
        headers={
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
        json={},
        timeout=300,
    )

    if response.status_code == 200:
        log.info("✅ wide_view_mv refreshed successfully")
    else:
        log.error(f"❌ Refresh failed: {response.status_code} - {response.text}")
        sys.exit(1)

if __name__ == "__main__":
    main()
