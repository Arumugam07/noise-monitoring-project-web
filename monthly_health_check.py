#!/usr/bin/env python3
"""
Weekly health check - runs every Monday, checks last 7 days.
Always sends a summary. Flags sensors by severity.
Consecutive checks:
  - 3+ consecutive days below 40% → persistent failure alert
  - 3+ consecutive days of ZERO readings → emergency vendor alert
"""

import os
import argparse
import logging
from datetime import datetime, timedelta
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from supabase_common import LOCATIONS
from telegram_alert import send_telegram_message

# ==========================================================
READINGS_PER_DAY = 1440
CRITICAL_THRESHOLD = 0.40   # Below 40% = CRITICAL
WARNING_THRESHOLD = 0.85    # Below 85% = WARNING, above 85% = HEALTHY
CONSECUTIVE_DAYS = 3        # 3+ consecutive days = flag
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


def _send_stale_mv_alert(start_date, end_date):
    msg = (
        f"⚠️ <b>WEEKLY CHECK FAILED — No Data Found</b>\n\n"
        f"📅 Period checked: {start_date} → {end_date}\n\n"
        f"<code>wide_view_mv</code> returned 0 rows.\n"
        f"The materialized view likely hasn't been refreshed recently.\n\n"
        f"🔧 Fix — run in Supabase SQL Editor:\n"
        f"<code>REFRESH MATERIALIZED VIEW public.wide_view_mv;</code>"
    )
    log.warning(f"No data returned for {start_date} to {end_date} — MV may be stale")
    try:
        send_telegram_message(msg, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        log.info("✅ Stale MV alert sent via Telegram")
    except Exception as e:
        log.error(f"Could not send stale MV alert: {e}")


def analyse_sensors(df, start_date, end_date):
    critical = []
    warning = []
    healthy = []

    total_days = (end_date - start_date).days + 1
    expected_total = READINGS_PER_DAY * total_days

    location_cols = [c for c in df.columns if c not in ("Date", "Time")]

    for loc_id in location_cols:
        loc_name = LOCATION_MAP.get(loc_id, loc_id)

        total_readings = df[loc_id].notna().sum() if loc_id in df.columns else 0
        completeness_pct = (total_readings / expected_total * 100) if expected_total > 0 else 0

        days_offline = []
        days_degraded = []
        current_streak = 0
        max_streak = 0

        for single_date in pd.date_range(start_date, end_date, freq="D"):
            single_date = single_date.date()
            day_df = df[df["Date"] == single_date]
            day_count = day_df[loc_id].notna().sum() if (not day_df.empty and loc_id in day_df.columns) else 0

            if day_count == 0:
                days_offline.append(single_date.strftime("%b %d"))
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                current_streak = 0
                if (day_count / READINGS_PER_DAY * 100) < 30:
                    days_degraded.append(single_date.strftime("%b %d"))

        sensor = {
            "name": loc_name,
            "completeness_pct": round(completeness_pct, 1),
            "total_readings": total_readings,
            "expected_total": expected_total,
            "total_days": total_days,
            "days_offline": days_offline,
            "days_degraded": days_degraded,
            "max_consecutive_offline": max_streak,
            "has_consecutive_offline": max_streak >= CONSECUTIVE_DAYS,
        }

        if completeness_pct < CRITICAL_THRESHOLD * 100:
            critical.append(sensor)
        elif completeness_pct < WARNING_THRESHOLD * 100:
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
            consec_note = f" ⚠️ {s['max_consecutive_offline']} consecutive offline days!" if s["has_consecutive_offline"] else ""
            critical_lines += (
                f"  • <b>{s['name']}</b>\n"
                f"    Completeness: {s['completeness_pct']}% "
                f"({s['total_readings']:,}/{s['expected_total']:,} readings)\n"
                f"    Offline days: {offline_str}{consec_note}\n"
                f"    ⚠️ Possible hardware fault or connectivity issue\n\n"
            )

    warning_lines = ""
    if warning:
        warning_lines = "\n🟡 <b>WARNING — Degraded (40-85%):</b>\n"
        for s in warning:
            offline_str = ", ".join(s["days_offline"]) if s["days_offline"] else "None"
            degraded_str = ", ".join(s["days_degraded"]) if s["days_degraded"] else "None"
            consec_note = f" ⚠️ {s['max_consecutive_offline']} consecutive offline days!" if s["has_consecutive_offline"] else ""
            warning_lines += (
                f"  • <b>{s['name']}</b>\n"
                f"    Completeness: {s['completeness_pct']}% "
                f"({s['total_readings']:,}/{s['expected_total']:,} readings)\n"
                f"    Offline days: {offline_str}{consec_note}\n"
                f"    Degraded days: {degraded_str}\n"
                f"    👀 Monitor this sensor\n\n"
            )

    healthy_lines = ""
    if healthy:
        healthy_lines = "\n✅ <b>HEALTHY — Operating normally (≥85%):</b>\n"
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


def check_consecutive_critical(df, start_date, end_date):
    """Sensors with 3+ consecutive days below 40% completeness."""
    location_cols = [c for c in df.columns if c not in ("Date", "Time")]
    persistently_critical = []

    for loc_id in location_cols:
        loc_name = LOCATION_MAP.get(loc_id, loc_id)
        day_results = []

        for single_date in pd.date_range(start_date, end_date, freq="D"):
            single_date = single_date.date()
            day_df = df[df["Date"] == single_date]
            day_count = (
                day_df[loc_id].notna().sum()
                if (not day_df.empty and loc_id in day_df.columns)
                else 0
            )
            day_pct = day_count / READINGS_PER_DAY
            day_results.append({
                "date": single_date,
                "pct": round(day_pct * 100, 1),
                "count": day_count
            })

        max_streak = 0
        current_streak = 0
        for d in day_results:
            if d["pct"] < CRITICAL_THRESHOLD * 100:
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                current_streak = 0

        if max_streak >= CONSECUTIVE_DAYS:
            persistently_critical.append({
                "name": loc_name,
                "days": day_results,
                "max_streak": max_streak,
            })

    return persistently_critical


def check_zero_reading_emergency(df, start_date, end_date):
    """Sensors with 3+ consecutive days of ZERO readings — likely dead solar battery."""
    location_cols = [c for c in df.columns if c not in ("Date", "Time")]
    emergency_sensors = []

    for loc_id in location_cols:
        loc_name = LOCATION_MAP.get(loc_id, loc_id)
        day_results = []

        for single_date in pd.date_range(start_date, end_date, freq="D"):
            single_date = single_date.date()
            day_df = df[df["Date"] == single_date]
            day_count = (
                day_df[loc_id].notna().sum()
                if (not day_df.empty and loc_id in day_df.columns)
                else 0
            )
            day_results.append({
                "date": single_date,
                "count": day_count
            })

        max_streak = 0
        current_streak = 0
        for d in day_results:
            if d["count"] == 0:  # Completely offline — zero readings
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                current_streak = 0

        if max_streak >= CONSECUTIVE_DAYS:
            emergency_sensors.append({
                "name": loc_name,
                "days": day_results,
                "max_streak": max_streak,
            })

    return emergency_sensors


def build_consecutive_alert(persistently_critical, start_date, end_date):
    sensor_lines = ""
    for s in persistently_critical:
        day_breakdown = " | ".join(
            f"{d['date'].strftime('%b %d')}: {d['pct']}%"
            for d in s["days"]
        )
        sensor_lines += (
            f"\n🔴 <b>{s['name']}</b>\n"
            f"    Max consecutive critical days: {s['max_streak']}\n"
            f"    Daily completeness: {day_breakdown}\n"
            f"    ⚠️ Hardware inspection required\n"
        )

    return f"""🚨 <b>PERSISTENT SENSOR FAILURE ALERT</b>
📍 RSAF Noise Monitoring System
📅 Period: {start_date} → {end_date}

The following {len(persistently_critical)} sensor(s) have been below 40% for <b>{CONSECUTIVE_DAYS}+ consecutive days</b>:
{sensor_lines}
━━━━━━━━━━━━━━━━━━━━━━
<i>Automated weekly failure check. Investigate immediately.</i>"""


def build_emergency_alert(emergency_sensors, start_date, end_date):
    sensor_lines = ""
    for s in emergency_sensors:
        day_breakdown = " | ".join(
            f"{d['date'].strftime('%b %d')}: {'❌ NO DATA' if d['count'] == 0 else '✅ OK'}"
            for d in s["days"]
        )
        sensor_lines += (
            f"\n🔴 <b>{s['name']}</b>\n"
            f"    {s['max_streak']} consecutive days with ZERO readings\n"
            f"    Daily status: {day_breakdown}\n"
            f"    🔋 Solar battery likely dead — vendor must attend site immediately\n"
        )

    return f"""🆘 <b>EMERGENCY — SENSOR COMPLETELY OFFLINE</b>
📍 RSAF Noise Monitoring System
📅 Period: {start_date} → {end_date}

The following {len(emergency_sensors)} sensor(s) have had <b>ZERO readings for {CONSECUTIVE_DAYS}+ consecutive days</b>:
{sensor_lines}
━━━━━━━━━━━━━━━━━━━━━━
⚡ <b>ACTION REQUIRED:</b> Vendor must physically attend these sites to inspect and recharge the solar-powered sensors.
━━━━━━━━━━━━━━━━━━━━━━
<i>Automated emergency alert.</i>"""


def main():
    log.info("Running weekly sensor health check")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    df, start_date, end_date = fetch_last_7_days(supabase)

    if df.empty:
        _send_stale_mv_alert(start_date, end_date)
        return

    critical, warning, healthy = analyse_sensors(df, start_date, end_date)
    log.info(f"Critical: {len(critical)} | Warning: {len(warning)} | Healthy: {len(healthy)}")

    message = build_weekly_message(critical, warning, healthy, start_date, end_date)

    try:
        send_telegram_message(message, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        log.info("✅ Weekly report sent successfully")
    except Exception as e:
        log.error(f"❌ Failed to send report: {e}")


def run_consecutive_check():
    log.info("Running consecutive critical sensor check")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    df, start_date, end_date = fetch_last_7_days(supabase)

    if df.empty:
        _send_stale_mv_alert(start_date, end_date)
        return

    # Check 1: below 40% for 3+ consecutive days
    persistently_critical = check_consecutive_critical(df, start_date, end_date)
    if not persistently_critical:
        log.info("✅ No sensors with 3+ consecutive critical days.")
    else:
        log.info(f"🚨 {len(persistently_critical)} sensor(s) persistently critical — sending alert")
        message = build_consecutive_alert(persistently_critical, start_date, end_date)
        try:
            send_telegram_message(message, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
            log.info("✅ Consecutive failure alert sent")
        except Exception as e:
            log.error(f"❌ Failed to send alert: {e}")

    # Check 2: ZERO readings for 3+ consecutive days — emergency vendor alert
    emergency_sensors = check_zero_reading_emergency(df, start_date, end_date)
    if not emergency_sensors:
        log.info("✅ No sensors with 3+ consecutive days of zero readings.")
    else:
        log.info(f"🆘 {len(emergency_sensors)} sensor(s) completely offline — sending EMERGENCY alert")
        message = build_emergency_alert(emergency_sensors, start_date, end_date)
        try:
            send_telegram_message(message, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
            log.info("✅ Emergency alert sent")
        except Exception as e:
            log.error(f"❌ Failed to send emergency alert: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--consecutive-check", action="store_true",
                        help="Run consecutive failure check instead of weekly report")
    args = parser.parse_args()

    if args.consecutive_check:
        run_consecutive_check()
    else:
        main()
