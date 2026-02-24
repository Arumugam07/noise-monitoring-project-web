#!/usr/bin/env python3
"""
Daily health check - detects 7 consecutive days below 40% and alerts.
Runs daily, checks the last 7 days only.
"""

import os
import logging
from datetime import datetime, timedelta
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from supabase_common import LOCATIONS
from telegram_alert import send_telegram_message, send_telegram_photo
from health_screenshot import screenshot_streamlit_health

# ==========================================================
# CONFIGURATION
# ==========================================================

OFFLINE_THRESHOLD = 0.40
CONSECUTIVE_DAYS_REQUIRED = 7
READINGS_PER_DAY = 1440

# ==========================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("daily-health-check")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

LOCATION_MAP = {loc["ID"]: loc["Name"] for loc in LOCATIONS}


def fetch_last_7_days(supabase):

    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=6)

    all_data = []
    offset = 0

    while True:
        resp = (
            supabase.table("wide_view_mv")
            .select("*")
            .gte("Date", str(start_date))
            .lte("Date", str(end_date))
            .range(offset, offset + 999)
            .execute()
        )

        batch = resp.data or []
        if not batch:
            break

        all_data.extend(batch)

        if len(batch) < 1000:
            break

        offset += 1000

    df = pd.DataFrame(all_data)

    if not df.empty:
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

    log.info(f"Fetched {len(df)} rows from {start_date} to {end_date}")
    return df, start_date, end_date


def detect_offline_sensors(df, start_date, end_date):

    alerts = []
    location_cols = [c for c in df.columns if c not in ("Date", "Time")]

    for loc_id in location_cols:
        loc_name = LOCATION_MAP.get(loc_id, loc_id)
        days_offline = 0

        for single_date in pd.date_range(start_date, end_date, freq="D"):
            single_date = single_date.date()
            day_df = df[df["Date"] == single_date]

            if day_df.empty or loc_id not in day_df.columns:
                day_count = 0
            else:
                day_count = day_df[loc_id].notna().sum()

            completeness = day_count / READINGS_PER_DAY

            if completeness < OFFLINE_THRESHOLD:
                days_offline += 1

        if days_offline >= CONSECUTIVE_DAYS_REQUIRED:
            alerts.append({
                "location_name": loc_name,
                "offline_start": start_date,
                "offline_end": end_date,
                "days_offline": days_offline
            })
            log.warning(f"⚠️ {loc_name}: {days_offline}/7 days below 40%")
        else:
            log.info(f"✅ {loc_name}: {days_offline}/7 days below 40% — no alert")

    return alerts


def build_alert_message(alerts, start_date, end_date):

    alert_lines = "\n".join([
        f"• <b>{a['location_name']}</b>\n"
        f"  📅 {a['offline_start']} → {a['offline_end']}\n"
        f"  ⏱ {a['days_offline']} consecutive days below 40%"
        for a in alerts
    ])

    return f"""🚨 <b>NOISE MONITORING SYSTEM ALERT</b>

📅 Checking period: {start_date} → {end_date}

━━━━━━━━━━━━━━━━━━━━━━
⚠️ <b>SENSORS OFFLINE (Below 40% for 7 Days)</b>

{alert_lines}

━━━━━━━━━━━━━━━━━━━━━━
⚡ Action required: Please check the affected sensors.
"""


def main():

    log.info("Running daily 7-day health check")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    df, start_date, end_date = fetch_last_7_days(supabase)

    if df.empty:
        log.warning("No data returned — skipping check")
        return

    alerts = detect_offline_sensors(df, start_date, end_date)

    if not alerts:
        log.info("✅ No sensors offline for 7 consecutive days — no alert sent")
        return

    log.warning(f"🚨 {len(alerts)} sensor(s) triggered alert — sending Telegram notification")

    screenshot_path = screenshot_streamlit_health("health_alert.png")
    message = build_alert_message(alerts, start_date, end_date)

    try:
        send_telegram_photo(
            image_path=screenshot_path,
            caption="🚨 Sensor Offline Alert — 7 Days Below 40%",
            token=TELEGRAM_TOKEN,
            chat_id=TELEGRAM_CHAT_ID
        )
        send_telegram_message(
            message,
            TELEGRAM_TOKEN,
            TELEGRAM_CHAT_ID
        )
        log.info("✅ Alert sent successfully")

    except Exception as e:
        log.error(f"❌ Failed to send alert: {e}")


if __name__ == "__main__":
    main()
