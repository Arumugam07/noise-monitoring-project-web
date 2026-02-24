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

    # Use YESTERDAY as end date so today's partial data is excluded
    # This gives full complete days only (e.g. if today is Feb 24,
    # we check Feb 17 → Feb 23 — all 7 days with full 1440 readings)
    yesterday = datetime.now().date() - timedelta(days=1)
    end_date = yesterday
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
    Match Streamlit app logic exactly:
    - completeness = total_readings / (READINGS_PER_DAY * total_days)
    - ONLINE >= 70%, DEGRADED 40-70%, CRITICAL < 40%
    """
    critical = []
    warning = []
    healthy = []

    total_days = (end_date - start_date).days + 1
    expected_total = READINGS_PER_DAY * total_days

    location_cols = [c for c in df.columns if c not in ("Date", "Time")]

    for loc_id in location_cols:
        loc_name = LOCATION_MAP.get(loc_id, loc_id)

        # Total readings for this sensor across all 7 days
        total_readings = df[loc_id].notna().sum() if loc_id in df.columns else 0

        # Completeness as one number — matches app exactly
        completeness_pct = (total_readings / expected_total * 100) if expected_total > 0 else 0

        # Count offline days (0 readings = offline)
        days_offline = []
        days_degraded = []
        for single_date in pd.date_range(start_date, end_date, freq="D"):
            single_date = single_date.date()
            day_df = df[df["Date"] == single_date]
            day_count = day_df[loc_id].notna().sum() if (not day_df.empty and loc_id in day_df.columns) else 0
            day_pct = day_count / READINGS_PER_DAY * 100

            if day_pct < 40:
                days_offline.append(single_date.strftime("%b %d"))
            elif day_pct < 70:
                days_degraded.append(single_date.strftime("%b %d"))

        sensor = {
            "name": loc_name,
            "completeness_pct": round(completeness_pct, 1),
            "total_readings": total_readings,
            "expected_total": expected_total,
            "total_days": total_days,
            "days_offline": days_offline,
            "days_degraded": days_degraded,
        }

        if completeness_pct < 40:
            critical.append(sensor)
        elif completeness_pct < 70:
            warning.append(sensor)
        else:
            healthy.append(sensor)

    return critical, warning, healthy


def build_weekly_message(critical, warning, healthy, start_date, end_date):

    total = len(critical) + len(warning) + len(healthy)

    if len(critical) == 0 and len(warning) == 0:
        overall = "✅ ALL SYSTEMS HEALTHY"
        overall_note = "All sensors performed well this week. No action needed."
    elif len(critical) == 0:
        overall = "⚠️ SYSTEM NEEDS ATTENTION"
        overall_note = "Some sensors are underperforming. Monitor closely."
    else:
        overall = "🚨 CRITICAL SENSORS DETECTED"
        overall_note = "Immediate inspection required for critical sensors."

    critical_lines = ""
    if critical:
        critical_lines = "\n🔴 <b>CRITICAL — Below 40% completeness:</b>\n"
        for s in critical:
            offline_str = ", ".join(s["days_offline"]) if s["days_offline"] else "None"
            critical_lines += (
                f"  • <b>{s['name']}</b>\n"
                f"    Completeness: {s['completeness_pct']}% "
                f"({s['total_readings']:,}/{s['expected_total']:,} readings)\n"
                f"    Offline days: {offline_str}\n"
                f"    ⚠️ Possible hardware fault or connectivity issue\n\n"
            )

    warning_lines = ""
    if warning:
        warning_lines = "\n🟡 <b>WARNING — Degraded (40-70%):</b>\n"
        for s in warning:
            offline_str = ", ".join(s["days_offline"]) if s["days_offline"] else "None"
            degraded_str = ", ".join(s["days_degraded"]) if s["days_degraded"] else "None"
            warning_lines += (
                f"  • <b>{s['name']}</b>\n"
                f"    Completeness: {s['completeness_pct']}% "
                f"({s['total_readings']:,}/{s['expected_total']:,} readings)\n"
                f"    Offline days: {offline_str}\n"
                f"    Degraded days: {degraded_str}\n"
                f"    👀 Monitor this sensor\n\n"
            )

    healthy_lines = ""
    if healthy:
        healthy_lines = "\n✅ <b>HEALTHY — Operating normally (≥70%):</b>\n"
        for s in healthy:
            healthy_lines += (
                f"  • {s['name']} — "
                f"{s['completeness_pct']}% "
                f"({s['total_readings']:,}/{s['expected_total']:,})\n"
            )

    return f"""📊 <b>WEEKLY SENSOR HEALTH REPORT</b>
📍 RSAF Noise Monitoring System
📅 Week: {start_date} → {end_date}

━━━━━━━━━━━━━━━━━━━━━━
{overall}
{overall_note}

━━━━━━━━━━━━━━━━━━━━━━
📈 <b>SUMMARY</b>

🔴 Critical:  {len(critical)}/{total} sensors
🟡 Warning:   {len(warning)}/{total} sensors
✅ Healthy:   {len(healthy)}/{total} sensors
{critical_lines}{warning_lines}{healthy_lines}
━━━━━━━━━━━━━━━━━━━━━━
<i>Automated weekly report. Next report in 7 days.</i>
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
