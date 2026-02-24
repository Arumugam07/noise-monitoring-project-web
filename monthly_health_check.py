#!/usr/bin/env python3
"""
Weekly health check - runs every Monday, checks last 7 days.
Always sends a summary. Flags sensors by severity.
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
READINGS_PER_DAY = 1440
CRITICAL_THRESHOLD = 0.40   # Below 40% = CRITICAL
WARNING_THRESHOLD = 0.70    # Below 70% = WARNING
# ==========================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("weekly-health-check")

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


def analyse_sensors(df, start_date, end_date):
    """
    For each sensor, calculate how many of the 7 days
    were below 40% (critical) or below 70% (warning).
    Returns three lists: critical, warning, healthy.
    """
    critical = []
    warning = []
    healthy = []

    location_cols = [c for c in df.columns if c not in ("Date", "Time")]

    for loc_id in location_cols:
        loc_name = LOCATION_MAP.get(loc_id, loc_id)
        days_critical = 0
        days_warning = 0
        days_healthy = 0
        daily_completeness = []

        for single_date in pd.date_range(start_date, end_date, freq="D"):
            single_date = single_date.date()
            day_df = df[df["Date"] == single_date]

            if day_df.empty or loc_id not in day_df.columns:
                day_count = 0
            else:
                day_count = day_df[loc_id].notna().sum()

            completeness = day_count / READINGS_PER_DAY
            daily_completeness.append(round(completeness * 100, 1))

            if completeness < CRITICAL_THRESHOLD:
                days_critical += 1
            elif completeness < WARNING_THRESHOLD:
                days_warning += 1
            else:
                days_healthy += 1

        avg_completeness = sum(daily_completeness) / len(daily_completeness)

        sensor = {
            "name": loc_name,
            "days_critical": days_critical,
            "days_warning": days_warning,
            "days_healthy": days_healthy,
            "avg_completeness": round(avg_completeness, 1),
            "daily": daily_completeness
        }

        if days_critical == 7:
            critical.append(sensor)
        elif days_critical >= 4 or days_warning >= 5:
            warning.append(sensor)
        else:
            healthy.append(sensor)

    return critical, warning, healthy


def build_weekly_message(critical, warning, healthy, start_date, end_date):

    total = len(critical) + len(warning) + len(healthy)

    # Determine overall system status
    if len(critical) == 0 and len(warning) == 0:
        overall = "✅ ALL SYSTEMS HEALTHY"
        overall_note = "All sensors are performing well this week. No action needed."
    elif len(critical) == 0:
        overall = "⚠️ SYSTEM NEEDS ATTENTION"
        overall_note = "Some sensors are underperforming. Monitor closely."
    else:
        overall = "🚨 CRITICAL SENSORS DETECTED"
        overall_note = "Immediate inspection required for critical sensors."

    # Build critical section
    critical_lines = ""
    if critical:
        critical_lines = "\n🔴 <b>CRITICAL — Below 40% for all 7 days:</b>\n"
        for s in critical:
            critical_lines += (
                f"  • <b>{s['name']}</b>\n"
                f"    Avg completeness: {s['avg_completeness']}%\n"
                f"    Days below 40%: {s['days_critical']}/7\n"
                f"    ⚠️ Possible hardware fault or connectivity issue\n"
            )

    # Build warning section
    warning_lines = ""
    if warning:
        warning_lines = "\n🟡 <b>WARNING — Degraded performance:</b>\n"
        for s in warning:
            warning_lines += (
                f"  • <b>{s['name']}</b>\n"
                f"    Avg completeness: {s['avg_completeness']}%\n"
                f"    Days critical: {s['days_critical']}/7 | "
                f"Days degraded: {s['days_warning']}/7\n"
                f"    👀 Keep an eye on this sensor\n"
            )

    # Build healthy section
    healthy_lines = ""
    if healthy:
        healthy_lines = "\n✅ <b>HEALTHY — Operating normally:</b>\n"
        for s in healthy:
            healthy_lines += f"  • {s['name']} ({s['avg_completeness']}% avg)\n"

    return f"""📊 <b>WEEKLY SENSOR HEALTH REPORT</b>
📍 RSAF Noise Monitoring System
📅 Week: {start_date} → {end_date}

━━━━━━━━━━━━━━━━━━━━━━
{overall}
{overall_note}

━━━━━━━━━━━━━━━━━━━━━━
📈 <b>SUMMARY</b>

🔴 Critical: {len(critical)}/{total} sensors
🟡 Warning:  {len(warning)}/{total} sensors
✅ Healthy:  {len(healthy)}/{total} sensors
{critical_lines}{warning_lines}{healthy_lines}
━━━━━━━━━━━━━━━━━━━━━━
<i>This is an automated weekly report. Next report in 7 days.</i>
"""


def main():

    log.info("Running weekly sensor health check")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    df, start_date, end_date = fetch_last_7_days(supabase)

    if df.empty:
        log.warning("No data returned — skipping check")
        return

    critical, warning, healthy = analyse_sensors(df, start_date, end_date)

    log.info(f"Critical: {len(critical)} | Warning: {len(warning)} | Healthy: {len(healthy)}")

    # Always send the weekly report regardless of status
    screenshot_path = screenshot_streamlit_health("health_alert.png")
    message = build_weekly_message(critical, warning, healthy, start_date, end_date)

    try:
        send_telegram_photo(
            image_path=screenshot_path,
            caption="📊 Weekly Sensor Health Report — RSAF Noise Monitoring",
            token=TELEGRAM_TOKEN,
            chat_id=TELEGRAM_CHAT_ID
        )
        send_telegram_message(
            message,
            TELEGRAM_TOKEN,
            TELEGRAM_CHAT_ID
        )
        log.info("✅ Weekly report sent successfully")

    except Exception as e:
        log.error(f"❌ Failed to send report: {e}")


if __name__ == "__main__":
    main()
