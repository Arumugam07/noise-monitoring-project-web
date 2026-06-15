#!/usr/bin/env python3
"""
Weekly health check - runs every 3 days, checks last 7 days.
Always sends a summary. Flags sensors by severity.

Fixes vs original:
- Distinguishes MV-empty (needs REFRESH) from ETL-gap (needs backfill)
- Correct Telegram alert diagnosis for each failure mode
- Consecutive checks now also validate data recency
"""

import os
import argparse
import logging
from datetime import datetime, timedelta, date
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
from supabase_common import LOCATIONS
from telegram_alert import send_telegram_message

# ==========================================================
READINGS_PER_DAY = 1440
CRITICAL_THRESHOLD = 0.40    # Below 40% = CRITICAL
WARNING_THRESHOLD  = 0.85    # Below 85% = WARNING, above = HEALTHY
CONSECUTIVE_DAYS   = 3       # 3+ consecutive bad days = flag
# ==========================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("weekly-health-check")

SUPABASE_URL     = os.getenv("SUPABASE_URL")
SUPABASE_KEY     = os.getenv("SUPABASE_ANON_KEY")
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

LOCATION_MAP = {loc["ID"]: loc["Name"] for loc in LOCATIONS}


# ── Data fetching ─────────────────────────────────────────────────────────────

def fetch_mv_total_count(supabase) -> int:
    """Check total rows in wide_view_mv regardless of date — tells us if MV is populated at all."""
    try:
        resp = supabase.table("wide_view_mv").select("Date", count="exact").limit(1).execute()
        return resp.count or 0
    except Exception as e:
        log.warning(f"Could not count MV rows: {e}")
        return -1  # -1 = query error (different from 0 = truly empty)


def fetch_mv_date_range(supabase):
    """Get the actual min/max Date in the MV so we know what's there."""
    try:
        resp = supabase.rpc("get_mv_date_range", {}).execute()
        if resp.data:
            return resp.data[0].get("min_date"), resp.data[0].get("max_date")
    except Exception:
        pass
    # Fallback: query directly
    try:
        min_resp = (supabase.table("wide_view_mv")
                    .select("Date").order("Date", desc=False).limit(1).execute())
        max_resp = (supabase.table("wide_view_mv")
                    .select("Date").order("Date", desc=True).limit(1).execute())
        min_date = min_resp.data[0]["Date"] if min_resp.data else None
        max_date = max_resp.data[0]["Date"] if max_resp.data else None
        return min_date, max_date
    except Exception as e:
        log.warning(f"Could not fetch MV date range: {e}")
        return None, None


def fetch_date_range(supabase, start_date, end_date):
    """Fetch wide_view_mv rows for the given date range, paginated."""
    all_data = []
    offset   = 0

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
    return df


def get_check_period() -> tuple[date, date]:
    yesterday  = datetime.now().date() - timedelta(days=1)
    start_date = yesterday - timedelta(days=6)
    return start_date, yesterday


# ── Telegram alert builders ───────────────────────────────────────────────────

def _send_empty_mv_alert(start_date, end_date):
    """MV has 0 rows at all — needs REFRESH."""
    msg = (
        f"⚠️ <b>WEEKLY CHECK FAILED — Materialized View Is Empty</b>\n\n"
        f"📅 Period checked: {start_date} → {end_date}\n\n"
        f"<code>wide_view_mv</code> has <b>zero rows total</b>.\n"
        f"The materialized view has never been populated or was accidentally dropped.\n\n"
        f"🔧 <b>Fix — run in Supabase SQL Editor:</b>\n"
        f"<code>REFRESH MATERIALIZED VIEW public.wide_view_mv;</code>\n\n"
        f"Then re-run the health check workflow."
    )
    log.warning("MV is completely empty — sending REFRESH alert")
    _telegram(msg)


def _send_etl_gap_alert(start_date, end_date, mv_min_date, mv_max_date):
    """MV has data, but not for the check period — ETL hasn't run recently."""
    gap_days = (end_date - mv_max_date).days if mv_max_date else "unknown"
    msg = (
        f"⚠️ <b>WEEKLY CHECK FAILED — ETL Gap Detected</b>\n\n"
        f"📅 Period checked: {start_date} → {end_date}\n"
        f"📦 MV data available: {mv_min_date} → {mv_max_date}\n\n"
        f"The materialized view has data but it's <b>{gap_days} days behind</b>.\n"
        f"The hourly/daily ETL has not been running successfully.\n\n"
        f"🔧 <b>Steps to fix:</b>\n"
        f"1. Check GitHub Actions → Hourly Noise ETL for errors\n"
        f"2. Verify <code>API_BASE_URL</code> secret is correct\n"
        f"3. Manually trigger <b>Backfill Noise Data</b> workflow\n"
        f"   with <code>BACKFILL_START_DATE={mv_max_date}</code>\n"
        f"4. After backfill, trigger <b>Refresh MV</b> workflow\n\n"
        f"<i>MV does NOT need REFRESH — it needs new data first.</i>"
    )
    log.warning(f"ETL gap: MV ends at {mv_max_date}, check period starts {start_date}")
    _telegram(msg)


def _send_mv_query_error_alert(start_date, end_date):
    """Could not even query the MV — connectivity or permission issue."""
    msg = (
        f"⚠️ <b>WEEKLY CHECK FAILED — Cannot Query Database</b>\n\n"
        f"📅 Period checked: {start_date} → {end_date}\n\n"
        f"Could not connect to or query <code>wide_view_mv</code>.\n\n"
        f"🔧 <b>Check:</b>\n"
        f"• <code>SUPABASE_URL</code> secret is correct\n"
        f"• <code>SUPABASE_ANON_KEY</code> secret is valid\n"
        f"• Supabase project is online at supabase.com"
    )
    log.error("MV query returned error — sending connectivity alert")
    _telegram(msg)


def _telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        log.warning("Telegram credentials not set — printing message instead:")
        log.warning(msg)
        return
    try:
        send_telegram_message(msg, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
        log.info("✅ Telegram alert sent")
    except Exception as e:
        log.error(f"❌ Failed to send Telegram message: {e}")


# ── Sensor analysis ───────────────────────────────────────────────────────────

def analyse_sensors(df, start_date, end_date):
    critical, warning, healthy = [], [], []
    total_days     = (end_date - start_date).days + 1
    expected_total = READINGS_PER_DAY * total_days
    location_cols  = [c for c in df.columns if c not in ("Date", "Time")]

    for loc_id in location_cols:
        loc_name       = LOCATION_MAP.get(loc_id, loc_id)
        total_readings = int(df[loc_id].notna().sum()) if loc_id in df.columns else 0
        completeness   = (total_readings / expected_total * 100) if expected_total > 0 else 0

        days_offline   = []
        days_degraded  = []
        current_streak = 0
        max_streak     = 0

        for dt in pd.date_range(start_date, end_date, freq="D"):
            d      = dt.date()
            day_df = df[df["Date"] == d]
            count  = int(day_df[loc_id].notna().sum()) if (not day_df.empty and loc_id in day_df.columns) else 0

            if count == 0:
                days_offline.append(d.strftime("%b %d"))
                current_streak += 1
                max_streak = max(max_streak, current_streak)
            else:
                current_streak = 0
                pct = count / READINGS_PER_DAY * 100
                if pct < 30:
                    days_degraded.append(d.strftime("%b %d"))

        sensor = {
            "name":                    loc_name,
            "completeness_pct":        round(completeness, 1),
            "total_readings":          total_readings,
            "expected_total":          expected_total,
            "total_days":              total_days,
            "days_offline":            days_offline,
            "days_degraded":           days_degraded,
            "max_consecutive_offline": max_streak,
            "has_consecutive_offline": max_streak >= CONSECUTIVE_DAYS,
        }

        if completeness < CRITICAL_THRESHOLD * 100:
            critical.append(sensor)
        elif completeness < WARNING_THRESHOLD * 100:
            warning.append(sensor)
        else:
            healthy.append(sensor)

    return critical, warning, healthy


# ── Report builder ────────────────────────────────────────────────────────────

def build_weekly_message(critical, warning, healthy, start_date, end_date):
    total = len(critical) + len(warning) + len(healthy)

    if not critical and not warning:
        overall      = "✅ ALL SYSTEMS HEALTHY"
        overall_note = "All sensors performed well this week. No action needed."
    elif not critical:
        overall      = "⚠️ SYSTEM NEEDS ATTENTION"
        overall_note = "Some sensors are underperforming. Monitor closely."
    else:
        overall      = "🚨 CRITICAL SENSORS DETECTED"
        overall_note = "Immediate inspection required for critical sensors."

    def _offline_str(s):
        return ", ".join(s["days_offline"]) if s["days_offline"] else "None"

    def _consec_note(s):
        return f" ⚠️ {s['max_consecutive_offline']} consecutive offline days!" if s["has_consecutive_offline"] else ""

    critical_lines = ""
    if critical:
        critical_lines = "\n🔴 <b>CRITICAL — Below 40% completeness:</b>\n"
        for s in critical:
            critical_lines += (
                f"  • <b>{s['name']}</b>\n"
                f"    Completeness: {s['completeness_pct']}% "
                f"({s['total_readings']:,}/{s['expected_total']:,} readings)\n"
                f"    Offline days: {_offline_str(s)}{_consec_note(s)}\n"
                f"    ⚠️ Possible hardware fault or connectivity issue\n\n"
            )

    warning_lines = ""
    if warning:
        warning_lines = "\n🟡 <b>WARNING — Degraded (40–85%):</b>\n"
        for s in warning:
            degraded_str = ", ".join(s["days_degraded"]) if s["days_degraded"] else "None"
            warning_lines += (
                f"  • <b>{s['name']}</b>\n"
                f"    Completeness: {s['completeness_pct']}% "
                f"({s['total_readings']:,}/{s['expected_total']:,} readings)\n"
                f"    Offline days: {_offline_str(s)}{_consec_note(s)}\n"
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

    return (
        f"📊 <b>WEEKLY SENSOR HEALTH REPORT</b>\n"
        f"📍 Noise Monitoring System\n"
        f"📅 Week: {start_date} → {end_date}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{overall}\n"
        f"{overall_note}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📈 <b>SUMMARY</b>\n\n"
        f"🔴 Critical:  {len(critical)}/{total} sensors\n"
        f"🟡 Warning:   {len(warning)}/{total} sensors\n"
        f"✅ Healthy:   {len(healthy)}/{total} sensors\n"
        f"{critical_lines}{warning_lines}{healthy_lines}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Automated report. Next report in 3 days.</i>"
    )


# ── Consecutive / emergency checks ───────────────────────────────────────────

def _day_counts(df, loc_id, start_date, end_date):
    results = []
    for dt in pd.date_range(start_date, end_date, freq="D"):
        d      = dt.date()
        day_df = df[df["Date"] == d]
        count  = int(day_df[loc_id].notna().sum()) if (not day_df.empty and loc_id in day_df.columns) else 0
        results.append({"date": d, "count": count, "pct": round(count / READINGS_PER_DAY * 100, 1)})
    return results


def check_consecutive_critical(df, start_date, end_date):
    location_cols = [c for c in df.columns if c not in ("Date", "Time")]
    flagged = []
    for loc_id in location_cols:
        loc_name = LOCATION_MAP.get(loc_id, loc_id)
        days     = _day_counts(df, loc_id, start_date, end_date)
        streak   = max_streak = 0
        for d in days:
            if d["pct"] < CRITICAL_THRESHOLD * 100:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 0
        if max_streak >= CONSECUTIVE_DAYS:
            flagged.append({"name": loc_name, "days": days, "max_streak": max_streak})
    return flagged


def check_zero_reading_emergency(df, start_date, end_date):
    location_cols = [c for c in df.columns if c not in ("Date", "Time")]
    flagged = []
    for loc_id in location_cols:
        loc_name = LOCATION_MAP.get(loc_id, loc_id)
        days     = _day_counts(df, loc_id, start_date, end_date)
        streak   = max_streak = 0
        for d in days:
            if d["count"] == 0:
                streak += 1
                max_streak = max(max_streak, streak)
            else:
                streak = 0
        if max_streak >= CONSECUTIVE_DAYS:
            flagged.append({"name": loc_name, "days": days, "max_streak": max_streak})
    return flagged


def build_consecutive_alert(sensors, start_date, end_date):
    lines = ""
    for s in sensors:
        breakdown = " | ".join(f"{d['date'].strftime('%b %d')}: {d['pct']}%" for d in s["days"])
        lines += (
            f"\n🔴 <b>{s['name']}</b>\n"
            f"    Max consecutive critical days: {s['max_streak']}\n"
            f"    Daily completeness: {breakdown}\n"
            f"    ⚠️ Hardware inspection required\n"
        )
    return (
        f"🚨 <b>PERSISTENT SENSOR FAILURE ALERT</b>\n"
        f"📍 Noise Monitoring System\n"
        f"📅 Period: {start_date} → {end_date}\n\n"
        f"The following {len(sensors)} sensor(s) have been below 40% "
        f"for <b>{CONSECUTIVE_DAYS}+ consecutive days</b>:\n"
        f"{lines}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Automated weekly failure check. Investigate immediately.</i>"
    )


def build_emergency_alert(sensors, start_date, end_date):
    lines = ""
    for s in sensors:
        breakdown = " | ".join(
            f"{d['date'].strftime('%b %d')}: {'❌ NO DATA' if d['count'] == 0 else '✅ OK'}"
            for d in s["days"]
        )
        lines += (
            f"\n🔴 <b>{s['name']}</b>\n"
            f"    {s['max_streak']} consecutive days with ZERO readings\n"
            f"    Daily status: {breakdown}\n"
            f"    🔋 Solar battery likely dead — vendor must attend site immediately\n"
        )
    return (
        f"🆘 <b>EMERGENCY — SENSOR COMPLETELY OFFLINE</b>\n"
        f"📍 Noise Monitoring System\n"
        f"📅 Period: {start_date} → {end_date}\n\n"
        f"The following {len(sensors)} sensor(s) have had "
        f"<b>ZERO readings for {CONSECUTIVE_DAYS}+ consecutive days</b>:\n"
        f"{lines}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⚡ <b>ACTION REQUIRED:</b> Vendor must physically attend these sites "
        f"to inspect and recharge the solar-powered sensors.\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Automated emergency alert.</i>"
    )


# ── Entry points ──────────────────────────────────────────────────────────────

def _validate_and_fetch(supabase, start_date, end_date):
    """
    Returns (df, ok) where ok=False means we already sent an alert and caller should exit.

    Three failure modes, each with a different Telegram message:
      1. MV query error        → connectivity / credentials problem
      2. MV has 0 rows total   → MV needs REFRESH
      3. MV has data but NOT for the check period → ETL gap, needs backfill
    """
    # Step 1: can we even reach the MV?
    total_count = fetch_mv_total_count(supabase)
    if total_count == -1:
        _send_mv_query_error_alert(start_date, end_date)
        return None, False

    # Step 2: is the MV completely empty?
    if total_count == 0:
        _send_empty_mv_alert(start_date, end_date)
        return None, False

    # Step 3: fetch date-filtered data
    df = fetch_date_range(supabase, start_date, end_date)

    # Step 4: date-filtered result is empty → ETL gap
    if df.empty:
        mv_min, mv_max = fetch_mv_date_range(supabase)
        _send_etl_gap_alert(start_date, end_date, mv_min, mv_max)
        return None, False

    return df, True


def main():
    log.info("Running weekly sensor health check")
    start_date, end_date = get_check_period()
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    df, ok = _validate_and_fetch(supabase, start_date, end_date)
    if not ok:
        return

    critical, warning, healthy = analyse_sensors(df, start_date, end_date)
    log.info(f"Critical: {len(critical)} | Warning: {len(warning)} | Healthy: {len(healthy)}")

    message = build_weekly_message(critical, warning, healthy, start_date, end_date)
    _telegram(message)
    log.info("✅ Weekly report sent successfully")


def run_consecutive_check():
    log.info("Running consecutive critical sensor check")
    start_date, end_date = get_check_period()
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    df, ok = _validate_and_fetch(supabase, start_date, end_date)
    if not ok:
        return

    # Check 1: below 40% for 3+ consecutive days
    persistently_critical = check_consecutive_critical(df, start_date, end_date)
    if persistently_critical:
        log.info(f"🚨 {len(persistently_critical)} sensor(s) persistently critical")
        _telegram(build_consecutive_alert(persistently_critical, start_date, end_date))
    else:
        log.info("✅ No sensors with 3+ consecutive critical days.")

    # Check 2: ZERO readings for 3+ consecutive days
    emergency_sensors = check_zero_reading_emergency(df, start_date, end_date)
    if emergency_sensors:
        log.info(f"🆘 {len(emergency_sensors)} sensor(s) completely offline")
        _telegram(build_emergency_alert(emergency_sensors, start_date, end_date))
    else:
        log.info("✅ No sensors with 3+ consecutive days of zero readings.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--consecutive-check", action="store_true",
                        help="Run consecutive failure check instead of weekly report")
    args = parser.parse_args()

    if args.consecutive_check:
        run_consecutive_check()
    else:
        main()
