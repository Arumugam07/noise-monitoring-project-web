#!/usr/bin/env python3
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("refresh-mv")

def main():
    log.info("✅ Refresh handled by Supabase pg_cron — no action needed")

if __name__ == "__main__":
    main()
