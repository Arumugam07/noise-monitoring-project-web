#!/usr/bin/env python3
"""
Monthly health check - detects 7+ consecutive offline days
and sends Telegram alert with screenshot.

TEST MODE ENABLED (for verifying Telegram + screenshot)
"""

import os
import sys
import logging
from datetime import datetime, timedelta, date
from calendar import monthrange
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from supabase_common import LOCATIONS
from telegram_alert import send_telegram_message, send_telegram_photo
from health_screenshot import screenshot_streamlit_health

# ==========================================================
# CONFIGURATION
# ==========================================================

TEST_MODE = False                 # 🔥 CHANGE TO False AFTER TESTING
OFFLINE_THRESHOLD = 0.40         # 40% completeness threshold
CONSECUTIVE_DAYS_REQUIRED = 7    # 7 consecutive days

READINGS_PER_DAY = 1440

# ==========================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("monthly-health-check")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

LOCATION_MAP = {loc["ID"]: loc["Name"] for loc in LOCATIONS}


# ----------------------------------------------------------
# FETCH DATA
# ----------------------------------------------------------
def fetch_month_data(supabase, year, month):

    first_day = date(year, month, 1)
    last_day = date(year, month, monthrange(year, month)[1])

    all_data = []
    offset = 0

    while True:
        resp = (
            supabase.table("meter_readings")
            .select("location_id, location_name, reading_value, reading_datetime")
            .gte("reading_datetime", f"{first_day}T00:00:00+00:00")
            .lte("reading_datetime", f"{last_day}T23:59:59+00:00")
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
        df["reading_datetime"] = pd.to_datetime(df["reading_datetime"], utc=True)
        df["Date"] = df["reading_datetime"].dt.tz_convert("Asia/Singapore").dt.date
        df["reading_datetime"] = df["reading_datetime"].dt.floor("min")

    return df, first_day, last_day


# ----------------------------------------------------------
# DETECT OFFLINE STREAKS (40% for 7 days)
# ----------------------------------------------------------
def detect_offline_streaks(df, first_day, last_day):

    alerts = []

    for loc_id, loc_name in LOCATION_MAP.items():

        loc_df = df[df["location_id"] == loc_id]
        consecutive = 0
        streak_start = None

        for single_date in pd.date_range(first_day, last_day, freq="D"):
            single_date = single_date.date()

            day_count = len(loc_df[loc_df["Date"] == single_date])
            completeness = day_count / READINGS_PER_DAY

            if completeness < OFFLINE_THRESHOLD:
                if consecutive == 0:
                    streak_start = single_date
                consecutive += 1
            else:
                if consecutive >= CONSECUTIVE_DAYS_REQUIRED:
                    alerts.append({
                        "location_name": loc_name,
                        "offline_start": streak_start,
                        "offline_end": single_date - timedelta(days=1),
                        "consecutive_days": consecutive
                    })
                consecutive = 0
                streak_start = None

        if consecutive >= CONSECUTIVE_DAYS_REQUIRED:
            alerts.append({
                "location_name": loc_name,
                "offline_start": streak_start,
                "offline_end": last_day,
                "consecutive_days": consecutive
            })

    return alerts


# ----------------------------------------------------------
# BUILD HEALTH SUMMARY
# ----------------------------------------------------------
def build_health_summary(df, first_day, last_day):

    total_days = (last_day - first_day).days + 1
    rows = []

    for loc_id, loc_name in LOCATION_MAP.items():

        loc_df = df[df["location_id"] == loc_id]
        total_readings = len(loc_df)
        expected = READINGS_PER_DAY * total_days
        completeness = (total_readings / expected * 100) if expected else 0
        days_online = loc_df["Date"].nunique() if not loc_df.empty else 0

        if completeness >= 70:
            status = "ONLINE"
        elif completeness >= 40:
            status = "DEGRADED"
        else:
            status = "OFFLINE"

        rows.append({
            "Location": loc_name,
            "Days_Online": days_online,
            "Completeness_%": round(completeness, 2),
            "Status": status
        })

    return pd.DataFrame(rows)


# ----------------------------------------------------------
# BUILD TELEGRAM MESSAGE
# ----------------------------------------------------------
def build_alert_message(offline_alerts, health_df, first_day, last_day):

    online = len(health_df[health_df["Status"] == "ONLINE"])
    degraded = len(health_df[health_df["Status"] == "DEGRADED"])
    offline = len(health_df[health_df["Status"] == "OFFLINE"])
    total = len(health_df)

    health_pct = (online / total * 100) if total else 0

    alert_lines = "\n".join([
        f"• <b>{a['location_name']}</b>\n"
        f"  📅 {a['offline_start']} → {a['offline_end']}\n"
        f"  ⏱ {a['consecutive_days']} consecutive days"
        for a in offline_alerts
    ])

    return f"""🚨 <b>NOISE MONITORING SYSTEM ALERT</b>

📅 {first_day} → {last_day}

━━━━━━━━━━━━━━━━━━━━━━
⚠️ <b>OFFLINE (Below 40% for 7 Days)</b>

{alert_lines}

━━━━━━━━━━━━━━━━━━━━━━
📊 <b>SYSTEM HEALTH</b>

🟢 Operational: {online}/{total}
🟡 Degraded: {degraded}/{total}
🔴 Critical: {offline}/{total}

📈 Overall Health: <b>{health_pct:.0f}%</b>
"""


# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------
def main():

    today = datetime.now()
    last_month = today.replace(day=1) - timedelta(days=1)
    year, month = last_month.year, last_month.month

    log.info("Running monthly health check")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    df, first_day, last_day = fetch_month_data(supabase, year, month)

    health_df = build_health_summary(df, first_day, last_day)

    offline_alerts = detect_offline_streaks(df, first_day, last_day)

    # 🔥 FORCE ALERT IN TEST MODE
    if TEST_MODE:
        log.warning("⚠ TEST MODE ENABLED — forcing test alert")

        offline_alerts = [{
            "location_name": "TEST LOCATION",
            "offline_start": first_day,
            "offline_end": last_day,
            "consecutive_days": CONSECUTIVE_DAYS_REQUIRED
        }]

    if not offline_alerts:
        log.info("✅ No alert triggered")
        return

    # Screenshot
    screenshot_path = screenshot_streamlit_health("health_alert.png")

    message = build_alert_message(
        offline_alerts,
        health_df,
        first_day,
        last_day
    )

    # Short caption (under 1024 chars)
    short_caption = "🚨 Noise Monitoring Alert\nScreenshot Attached"

    try:
        send_telegram_photo(
            image_path=screenshot_path,
            caption=short_caption,
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
