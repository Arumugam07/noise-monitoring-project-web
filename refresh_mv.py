#!/usr/bin/env python3
import os
import logging
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("refresh-mv")

def main():
    supabase = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_ANON_KEY"])
    try:
        supabase.rpc("refresh_wide_view_mv").execute()
        log.info("✅ wide_view_mv refreshed")
    except Exception as e:
        log.error(f"❌ Refresh failed: {e}")

if __name__ == "__main__":
    main()
