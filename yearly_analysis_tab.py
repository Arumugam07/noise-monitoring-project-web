"""
yearly_analysis_tab.py
----------------------
Streamlit tab: Yearly Persisted Noise Analysis

Workflow:
  1. User picks locations + reviews/adjusts per-location presets.
  2. "Load Pre-computed" → fast read from monthly_persisted_summary table (ETL fills this).
  3. "Compute Now"       → fetches raw data month-by-month with a progress bar,
                           computes incidents in-memory, displays results immediately.
  4. Results show per-location or cross-location comparison charts + tables.

Key metric: avg_duration_per_incident — the average length of a single sustained
noise event in minutes. This answers "how long does a typical noise burst last?"
rather than "how many times did it happen?" or "what was the total pile-up?".
"""

import calendar
from datetime import date

import pandas as pd
import streamlit as st

READINGS_PER_DAY = 1440


# ── Helpers ───────────────────────────────────────────────────────────────────

def _human_duration(minutes: float) -> str:
    """Convert minutes to a readable string like '2 days 3 hrs' or '47 min'."""
    if minutes <= 0:
        return "0 min"
    minutes = int(round(minutes))
    days    = minutes // 1440
    hours   = (minutes % 1440) // 60
    mins    = minutes % 60
    parts   = []
    if days:  parts.append(f"{days} day{'s' if days > 1 else ''}")
    if hours: parts.append(f"{hours} hr{'s' if hours > 1 else ''}")
    if mins and not days:  parts.append(f"{mins} min")   # skip mins once we're at days level
    return " ".join(parts) if parts else "< 1 min"


def _months_to_process() -> list[tuple[int, int]]:
    """Return rolling 12-month window ending at current month, in chronological order."""
    today_month = date.today().replace(day=1)
    months = []
    for i in range(11, -1, -1):
        m = pd.Timestamp(today_month) - pd.DateOffset(months=i)
        months.append((m.year, m.month))
    return months   # already oldest → newest


def _month_label(yr: int, mo: int) -> str:
    return date(yr, mo, 1).strftime("%b %Y")


# ── Data fetching ─────────────────────────────────────────────────────────────

def _fetch_monthly_chunk(client, view_name: str, location_id: str,
                          year: int, month: int) -> pd.DataFrame:
    _, last_day = calendar.monthrange(year, month)
    start = date(year, month, 1)
    end   = date(year, month, last_day)

    all_data, offset, batch_size = [], 0, 1000
    while True:
        resp = (
            client.table(view_name)
            .select(f"Date,Time,{location_id}")
            .gte("Date", str(start))
            .lte("Date", str(end))
            .order("Date").order("Time")
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        all_data.extend(batch)
        if len(batch) < batch_size:
            break
        offset += batch_size

    return pd.DataFrame(all_data)


def _detect_incidents(df: pd.DataFrame, location_id: str,
                       min_db: float, max_db: float,
                       duration_minutes: int) -> list[dict]:
    """
    Return one dict per *continuous run* of readings in [min_db, max_db]
    that lasts at least duration_minutes consecutive minutes.

    Each dict has:
      duration  – length of the run in minutes
      peak_db   – highest reading in the run
      avg_db    – mean reading in the run
    """
    if df.empty or location_id not in df.columns:
        return []

    df = df.copy()
    df[location_id] = pd.to_numeric(df[location_id], errors="coerce")

    vals     = df[location_id]
    in_range = vals.between(min_db, max_db, inclusive="both").fillna(False)
    group    = (in_range != in_range.shift()).cumsum().where(in_range)

    incidents = []
    for _gid, chunk in df.groupby(group):
        if len(chunk) < duration_minutes:
            continue
        v = chunk[location_id].dropna()
        if v.empty:
            continue
        incidents.append({
            "duration": len(chunk),
            "peak_db":  float(v.max()),
            "avg_db":   float(v.mean()),
        })
    return incidents


def _load_precomputed(client, location_ids: list[str],
                       start_month: date, end_month: date,
                       preset_overrides: dict) -> pd.DataFrame:
    all_records = []
    for loc_id in location_ids:
        p = preset_overrides[loc_id]
        try:
            resp = (
                client.table("monthly_persisted_summary")
                .select("*")
                .eq("location_id",      loc_id)
                .eq("min_db",           p["min_db"])
                .eq("max_db",           p["max_db"])
                .eq("duration_minutes", p["duration_minutes"])
                .gte("month", str(start_month))
                .lte("month", str(end_month))
                .execute()
            )
            all_records.extend(resp.data or [])
        except Exception as exc:
            st.warning(f"Could not load pre-computed data for {loc_id}: {exc}")

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df["month"] = pd.to_datetime(df["month"]).dt.date

    # Derive avg_duration_per_incident if not stored
    if "avg_duration_per_incident" not in df.columns:
        df["avg_duration_per_incident"] = df.apply(
            lambda r: round(r["total_duration_minutes"] / r["incident_count"], 1)
            if r["incident_count"] > 0 else 0.0, axis=1
        )
    return df


def _compute_fresh(client, view_name: str, selected_locs: list[str],
                    preset_overrides: dict) -> pd.DataFrame:
    from location_presets import LOCATION_PRESETS

    months      = _months_to_process()
    total_steps = len(selected_locs) * len(months)
    step        = 0

    progress_bar = st.progress(0.0)
    status_text  = st.empty()
    rows = []

    for loc_id in selected_locs:
        loc_name = LOCATION_PRESETS[loc_id]["name"]
        p        = preset_overrides[loc_id]

        for yr, mo in months:
            label = _month_label(yr, mo)
            status_text.markdown(f"⏳ **{loc_name}** — {label} ({step + 1}/{total_steps})")

            try:
                chunk     = _fetch_monthly_chunk(client, view_name, loc_id, yr, mo)
                incidents = _detect_incidents(chunk, loc_id, p["min_db"], p["max_db"], p["duration_minutes"])
            except Exception as exc:
                st.warning(f"Error fetching {loc_name} {label}: {exc}")
                incidents = []

            n        = len(incidents)
            total_d  = sum(i["duration"] for i in incidents)
            avg_dur  = round(total_d / n, 1) if n > 0 else 0.0
            avg_peak = round(sum(i["peak_db"] for i in incidents) / n, 1) if n > 0 else 0.0
            max_peak = round(max(i["peak_db"] for i in incidents), 1) if n > 0 else 0.0

            rows.append({
                "location_id":              loc_id,
                "location":                 loc_name,
                "month":                    date(yr, mo, 1),
                "month_label":              label,
                "incident_count":           n,
                "total_duration_minutes":   total_d,
                "avg_duration_per_incident": avg_dur,   # ← key new metric
                "avg_peak_db":              avg_peak,
                "max_peak_db":              max_peak,
                "min_db":                   p["min_db"],
                "max_db":                   p["max_db"],
                "duration_minutes":         p["duration_minutes"],
            })

            step += 1
            progress_bar.progress(step / total_steps)

    status_text.markdown("✅ **Computation complete!**")
    progress_bar.progress(1.0)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── Rendering ─────────────────────────────────────────────────────────────────

def _ordered_df(loc_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure rows are in chronological month order.
    Builds a complete 12-month spine so gaps show as 0, not missing bars.
    """
    month_window = [date(y, m, 1) for y, m in _months_to_process()]
    labels_in_order = [_month_label(d.year, d.month) for d in month_window]

    # Set month_label in the right order using a categorical so bar_chart respects it
    loc_df = loc_df.copy()
    loc_df["month_label"] = pd.Categorical(
        loc_df["month_label"], categories=labels_in_order, ordered=True
    )

    # Reindex to full 12-month spine, fill missing with 0
    spine = pd.DataFrame({"month": month_window,
                           "month_label": labels_in_order})
    merged = spine.merge(loc_df, on=["month", "month_label"], how="left").fillna(0)
    merged["month_label"] = pd.Categorical(
        merged["month_label"], categories=labels_in_order, ordered=True
    )
    return merged.sort_values("month_label")


def _render_single_location(loc_id: str, df: pd.DataFrame) -> None:
    from location_presets import LOCATION_PRESETS

    loc_name = LOCATION_PRESETS[loc_id]["name"]
    preset   = LOCATION_PRESETS[loc_id]

    loc_df = df[df["location_id"] == loc_id].copy() if "location_id" in df.columns else df.copy()
    loc_df["month"] = pd.to_datetime(loc_df["month"]).dt.date

    if loc_df.empty:
        st.info(f"No data found for **{loc_name}**.")
        return

    loc_df = _ordered_df(loc_df)

    # ── Derive avg_duration_per_incident if not present ───────────────────────
    if "avg_duration_per_incident" not in loc_df.columns:
        loc_df["avg_duration_per_incident"] = loc_df.apply(
            lambda r: round(r["total_duration_minutes"] / r["incident_count"], 1)
            if r["incident_count"] > 0 else 0.0, axis=1
        )

    # ── Header ────────────────────────────────────────────────────────────────
    st.markdown(f"#### 📍 {loc_name}")
    st.caption(
        f"Detection band: **{preset['min_db']}–{preset['max_db']} dB** "
        f"sustained for at least **{preset['duration_minutes']} consecutive minutes**. "
        f"An *incident* = one unbroken run of readings in that band lasting ≥ that threshold."
    )

    # ── Top-level KPIs ────────────────────────────────────────────────────────
    total_incidents = int(loc_df["incident_count"].sum())
    total_minutes   = int(loc_df["total_duration_minutes"].sum())
    non_zero        = loc_df[loc_df["incident_count"] > 0]

    overall_avg_dur = (
        round(loc_df[loc_df["avg_duration_per_incident"] > 0]["avg_duration_per_incident"].mean(), 1)
        if (loc_df["avg_duration_per_incident"] > 0).any() else 0.0
    )
    worst_month = (
        non_zero.loc[non_zero["avg_duration_per_incident"].idxmax(), "month_label"]
        if not non_zero.empty else "—"
    )

    m1, m2, m3, m4 = st.columns(4)
    m1.metric(
        "Avg Duration per Incident",
        _human_duration(overall_avg_dur),
        help="On average, how long does a single sustained noise event last?"
    )
    m2.metric(
        "Total Noise Time",
        _human_duration(total_minutes),
        help="All persisted noise minutes added together across the 12-month window."
    )
    m3.metric(
        "Total Incidents",
        f"{total_incidents:,}",
        help=f"Number of separate noise bursts lasting ≥ {preset['duration_minutes']} min in the detection band."
    )
    m4.metric(
        "Month with Longest Avg Burst",
        worst_month,
        help="The month where the average individual noise event was longest."
    )

    # ── Chart 1: Avg duration per incident (PRIMARY) ──────────────────────────
    st.markdown(
        "**📊 Average Duration per Incident — minutes** "
        "*(how long a typical noise burst lasted each month)*"
    )
    st.caption(
        "This answers: *'When noise happened, how long did it usually last?'* "
        "High bars = long individual events. Low bars = short bursts even if many occurred."
    )
    chart_df = loc_df.set_index("month_label")[["avg_duration_per_incident"]].copy()
    chart_df.index = chart_df.index.astype(str)   # strip Categorical for Streamlit
    st.bar_chart(chart_df)

    # ── Chart 2: Total noise minutes ──────────────────────────────────────────
    st.markdown(
        "**📊 Total Persisted Noise per Month — minutes** "
        "*(cumulative noise time, all incidents combined)*"
    )
    st.caption(
        "This answers: *'How much total noise time was there?'* "
        "Aug 2025 = ~3,900 min ≈ 2.7 days of continuous in-band noise across the whole month."
    )
    chart_df2 = loc_df.set_index("month_label")[["total_duration_minutes"]].copy()
    chart_df2.index = chart_df2.index.astype(str)
    st.bar_chart(chart_df2)

    # ── Chart 3: Incident count ───────────────────────────────────────────────
    st.markdown(
        "**📊 Number of Incidents per Month** "
        f"*(separate noise bursts ≥ {preset['duration_minutes']} min in {preset['min_db']}–{preset['max_db']} dB)*"
    )
    st.caption(
        "This answers: *'How often did sustained noise occur?'* "
        "Combine with Chart 1 to understand whether you had many short bursts or fewer long ones."
    )
    chart_df3 = loc_df.set_index("month_label")[["incident_count"]].copy()
    chart_df3.index = chart_df3.index.astype(str)
    st.bar_chart(chart_df3)

    # ── Monthly breakdown table ────────────────────────────────────────────────
    st.markdown("**Monthly Breakdown Table**")
    display = loc_df[[
        "month_label", "incident_count",
        "avg_duration_per_incident", "total_duration_minutes",
        "avg_peak_db", "max_peak_db"
    ]].copy()
    display["total_readable"] = display["total_duration_minutes"].apply(_human_duration)
    display["avg_readable"]   = display["avg_duration_per_incident"].apply(_human_duration)
    display = display[[
        "month_label", "incident_count",
        "avg_readable", "avg_duration_per_incident",
        "total_readable", "total_duration_minutes",
        "avg_peak_db", "max_peak_db"
    ]]
    display.columns = [
        "Month", "# Incidents",
        "Avg Duration", "Avg Duration (min)",
        "Total Noise Time", "Total (min)",
        "Avg Peak dB", "Max Peak dB"
    ]
    st.dataframe(display, use_container_width=True, hide_index=True)


def _render_multi_location(selected_locs: list[str], df: pd.DataFrame) -> None:
    from location_presets import LOCATION_PRESETS

    st.markdown("#### 🗺️ Cross-Location Comparison")

    if "location" not in df.columns:
        df = df.copy()
        df["location"] = df["location_id"].map(
            lambda x: LOCATION_PRESETS.get(x, {}).get("name", x)
        )

    # ── Per-location summary ──────────────────────────────────────────────────
    loc_summary = (
        df.groupby("location")
        .agg(
            total_incidents          =("incident_count",             "sum"),
            total_duration_minutes   =("total_duration_minutes",     "sum"),
            overall_avg_dur_per_inc  =("avg_duration_per_incident",  "mean"),
        )
        .reset_index()
        .sort_values("overall_avg_dur_per_inc", ascending=False)
    )
    loc_summary["overall_avg_dur_per_inc"] = loc_summary["overall_avg_dur_per_inc"].round(1)
    loc_summary["total_noise_readable"] = loc_summary["total_duration_minutes"].apply(_human_duration)
    loc_summary["avg_dur_readable"]     = loc_summary["overall_avg_dur_per_inc"].apply(_human_duration)

    st.markdown(
        "**Avg Duration per Incident by Location** "
        "*(which sites have the longest individual noise events?)*"
    )
    st.bar_chart(loc_summary.set_index("location")["overall_avg_dur_per_inc"])

    st.markdown("**Total Persisted Noise Minutes by Location**")
    st.bar_chart(loc_summary.set_index("location")["total_duration_minutes"])

    # ── Monthly heatmap ───────────────────────────────────────────────────────
    st.markdown("**Monthly Avg Duration Heatmap (rows = month, cols = location)** — values in minutes")
    df2 = df.copy()
    df2["month_label"] = pd.to_datetime(df2["month"]).dt.strftime("%b %Y")

    # Build ordered month list for index
    month_order = [_month_label(y, m) for y, m in _months_to_process()]

    heatmap = df2.pivot_table(
        index="month_label",
        columns="location",
        values="avg_duration_per_incident",
        aggfunc="mean",
        fill_value=0,
    ).round(1)
    heatmap = heatmap.reindex([m for m in month_order if m in heatmap.index])
    st.dataframe(heatmap, use_container_width=True)

    # ── Summary table ─────────────────────────────────────────────────────────
    st.markdown("**Location Summary**")
    disp = loc_summary[[
        "location", "total_incidents",
        "avg_dur_readable", "overall_avg_dur_per_inc",
        "total_noise_readable", "total_duration_minutes"
    ]].copy()
    disp.columns = [
        "Location", "# Incidents",
        "Avg Duration per Incident", "Avg (min)",
        "Total Noise Time", "Total (min)"
    ]
    st.dataframe(disp, use_container_width=True, hide_index=True)

    # ── Drilldown ─────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("#### 🔍 Per-Location Drilldown")
    drilldown_loc = st.selectbox(
        "Select a location for detailed monthly charts",
        options=selected_locs,
        format_func=lambda x: LOCATION_PRESETS[x]["name"],
        key="yearly_drilldown_loc",
    )
    _render_single_location(drilldown_loc, df)


def _render_results(summary_df: pd.DataFrame,
                     selected_locs: list[str],
                     range_label: str) -> None:
    st.markdown("---")
    st.markdown(f"### 📊 Results — {range_label}")

    if len(selected_locs) == 1:
        _render_single_location(selected_locs[0], summary_df)
    else:
        _render_multi_location(selected_locs, summary_df)

    st.markdown("---")
    st.markdown("### 📥 Export")
    export_cols = [c for c in summary_df.columns if c != "location_id"]
    csv = summary_df[export_cols].to_csv(index=False)
    st.download_button(
        label="📄 Download Summary CSV",
        data=csv,
        file_name=f"yearly_noise_summary_{range_label.replace(' ', '_')}.csv",
        mime="text/csv",
        use_container_width=True,
    )


# ── Main entry point ──────────────────────────────────────────────────────────

def show_yearly_analysis_tab(supabase_client, view_name: str) -> None:
    from location_presets import LOCATION_PRESETS

    st.markdown("### 📅 Yearly Persisted Noise Analysis")
    st.caption(
        "For each location, define the noise band and minimum sustained duration "
        "that counts as a meaningful event. The charts show you **how long** each "
        "event typically lasted — not just how many occurred or how many total minutes piled up."
    )

    # ── Location selector ─────────────────────────────────────────────────────
    all_loc_ids   = list(LOCATION_PRESETS.keys())
    selected_locs = st.multiselect(
        "📍 Locations",
        options=all_loc_ids,
        default=[all_loc_ids[0]],
        format_func=lambda x: LOCATION_PRESETS[x]["name"],
        help="Select one or more locations.",
        key="yearly_location_picker",
    )

    today_month = date.today().replace(day=1)
    start_month = (pd.Timestamp(today_month) - pd.DateOffset(months=11)).date()
    range_label = f"{start_month.strftime('%b %Y')} → {today_month.strftime('%b %Y')}"
    st.info(f"Showing rolling 12-month window: **{range_label}**")

    if not selected_locs:
        st.warning("Please select at least one location to continue.")
        return

    # ── Per-location preset editor ────────────────────────────────────────────
    st.markdown("#### ⚙️ Detection Thresholds per Location")
    st.caption(
        "An **incident** = one unbroken run of readings in the dB band below, "
        "lasting at least the minimum duration. Two readings outside the band "
        "break the run — the counter resets."
    )

    preset_overrides: dict[str, dict] = {}

    for i in range(0, len(selected_locs), 2):
        cols = st.columns(2)
        for j in range(2):
            if i + j >= len(selected_locs):
                break
            loc_id  = selected_locs[i + j]
            default = LOCATION_PRESETS[loc_id]

            with cols[j]:
                with st.expander(f"📍 {default['name']}", expanded=False):
                    st.caption(default.get("notes", ""))
                    min_db = st.number_input("Min dB", value=float(default["min_db"]),
                                              min_value=0.0, max_value=200.0, step=1.0,
                                              key=f"yr_min_{loc_id}")
                    max_db = st.number_input("Max dB", value=float(default["max_db"]),
                                              min_value=0.0, max_value=200.0, step=1.0,
                                              key=f"yr_max_{loc_id}")
                    dur    = st.number_input("Min Duration (min)", value=int(default["duration_minutes"]),
                                              min_value=1, max_value=60, step=1,
                                              key=f"yr_dur_{loc_id}")
                    preset_overrides[loc_id] = {"min_db": min_db, "max_db": max_db, "duration_minutes": dur}

    for loc_id in selected_locs:
        if loc_id not in preset_overrides:
            d = LOCATION_PRESETS[loc_id]
            preset_overrides[loc_id] = {"min_db": d["min_db"], "max_db": d["max_db"],
                                         "duration_minutes": d["duration_minutes"]}

    # ── Action buttons ────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown(
        "**⚡ Load Pre-computed** reads from the `monthly_persisted_summary` table "
        "(filled by the nightly ETL job) — instant. "
        "**🔄 Compute Now** fetches raw data month-by-month — ~1–3 minutes."
    )

    col_fast, col_slow = st.columns(2)
    load_btn    = col_fast.button("⚡ Load Pre-computed Data", use_container_width=True, type="primary")
    compute_btn = col_slow.button("🔄 Compute Now (Slow)",     use_container_width=True)

    cache_key = f"{sorted(selected_locs)}_{start_month}_{today_month}_{str(preset_overrides)}"

    if load_btn:
        with st.spinner("Loading pre-computed monthly summaries from Supabase…"):
            summary_df = _load_precomputed(supabase_client, selected_locs,
                                            start_month, today_month, preset_overrides)
        if summary_df.empty:
            st.warning(
                "No pre-computed summaries found for these exact settings. "
                "Run the ETL job first, or click **🔄 Compute Now**."
            )
        else:
            st.session_state.update({
                "yr_summary_df":  summary_df,
                "yr_cache_key":   cache_key,
                "yr_locs":        selected_locs,
                "yr_range_label": range_label,
            })

    if compute_btn:
        summary_df = _compute_fresh(supabase_client, view_name, selected_locs, preset_overrides)
        if not summary_df.empty:
            st.session_state.update({
                "yr_summary_df":  summary_df,
                "yr_cache_key":   cache_key,
                "yr_locs":        selected_locs,
                "yr_range_label": range_label,
            })

    if (
        "yr_summary_df" in st.session_state
        and st.session_state.get("yr_cache_key") == cache_key
    ):
        _render_results(
            st.session_state["yr_summary_df"],
            st.session_state["yr_locs"],
            st.session_state["yr_range_label"],
        )
