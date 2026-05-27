"""
yearly_analysis_tab.py
----------------------
Streamlit tab: Yearly Persisted Noise Analysis

The yearly view is built around three questions:
  1. How often did sustained noise happen?              -> incidents
  2. When it happened, how long did it usually last?    -> avg duration per incident
  3. How loud were those sustained events?              -> avg / max peak dB
"""

import calendar
from datetime import date

import altair as alt
import pandas as pd
import streamlit as st

READINGS_PER_DAY = 1440


# ── Helpers ───────────────────────────────────────────────────────────────────

def _human_duration(minutes: float) -> str:
    """Convert minutes to a readable string like '2 days 3 hrs' or '47 min'."""
    if pd.isna(minutes) or minutes <= 0:
        return "0 min"
    minutes = int(round(minutes))
    days = minutes // 1440
    hours = (minutes % 1440) // 60
    mins = minutes % 60
    parts = []
    if days:
        parts.append(f"{days} day{'s' if days > 1 else ''}")
    if hours:
        parts.append(f"{hours} hr{'s' if hours > 1 else ''}")
    if mins and not days:
        parts.append(f"{mins} min")
    return " ".join(parts) if parts else "< 1 min"


def _months_to_process() -> list[tuple[int, int]]:
    """Return rolling 12-month window ending at current month, in chronological order."""
    today_month = date.today().replace(day=1)
    months = []
    for i in range(11, -1, -1):
        m = pd.Timestamp(today_month) - pd.DateOffset(months=i)
        months.append((m.year, m.month))
    return months


def _month_label(yr: int, mo: int) -> str:
    return date(yr, mo, 1).strftime("%b %Y")


def _month_order() -> list[str]:
    return [_month_label(y, m) for y, m in _months_to_process()]


def _ensure_summary_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize loaded/computed monthly summary data before rendering."""
    df = df.copy()

    numeric_cols = [
        "incident_count", "total_duration_minutes", "avg_duration_per_incident",
        "avg_peak_db", "max_peak_db", "min_db", "max_db", "duration_minutes",
    ]
    for col in numeric_cols:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(float)

    needs_avg_duration = (df["avg_duration_per_incident"] <= 0) & (df["incident_count"] > 0)
    df.loc[needs_avg_duration, "avg_duration_per_incident"] = (
        df.loc[needs_avg_duration, "total_duration_minutes"]
        / df.loc[needs_avg_duration, "incident_count"]
    ).round(1)

    if "month" in df.columns:
        df["month"] = pd.to_datetime(df["month"]).dt.date
        df["month_label"] = pd.to_datetime(df["month"]).dt.strftime("%b %Y")

    return df


def _ranked_bar(
    data: pd.DataFrame,
    value_col: str,
    title: str,
    value_title: str,
    color: str = "#1f77b4",
    height_step: int = 34,
) -> None:
    """Readable horizontal bar chart for location ranking."""
    chart_df = data[["location", value_col]].copy().sort_values(value_col, ascending=True)
    chart_df[value_col] = pd.to_numeric(chart_df[value_col], errors="coerce").fillna(0)
    height = max(260, len(chart_df) * height_step)

    bars = (
        alt.Chart(chart_df)
        .mark_bar(color=color, cornerRadiusEnd=3)
        .encode(
            x=alt.X(f"{value_col}:Q", title=value_title, axis=alt.Axis(grid=True)),
            y=alt.Y("location:N", sort="-x", title=None, axis=alt.Axis(labelLimit=360)),
            tooltip=[
                alt.Tooltip("location:N", title="Location"),
                alt.Tooltip(f"{value_col}:Q", title=value_title, format=",.1f"),
            ],
        )
    )
    text = (
        alt.Chart(chart_df)
        .mark_text(align="left", baseline="middle", dx=4, color="#263238")
        .encode(
            x=alt.X(f"{value_col}:Q"),
            y=alt.Y("location:N", sort="-x"),
            text=alt.Text(f"{value_col}:Q", format=",.1f"),
        )
    )
    st.markdown(f"**{title}**")
    st.altair_chart((bars + text).properties(height=height), use_container_width=True)


def _month_bar(data: pd.DataFrame, value_col: str, title: str, value_title: str) -> None:
    chart_df = data[["month_label", value_col]].copy()
    chart_df[value_col] = pd.to_numeric(chart_df[value_col], errors="coerce").fillna(0)
    chart_df["month_label"] = pd.Categorical(chart_df["month_label"], categories=_month_order(), ordered=True)
    chart_df = chart_df.sort_values("month_label")

    chart = (
        alt.Chart(chart_df)
        .mark_bar(color="#1f77b4", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("month_label:N", sort=_month_order(), title=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y(f"{value_col}:Q", title=value_title),
            tooltip=[
                alt.Tooltip("month_label:N", title="Month"),
                alt.Tooltip(f"{value_col}:Q", title=value_title, format=",.1f"),
            ],
        )
        .properties(height=300)
    )
    st.markdown(f"**{title}**")
    st.altair_chart(chart, use_container_width=True)


def _format_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    display = df.copy()
    display["Avg Event Length"] = display["avg_duration_per_incident"].apply(_human_duration)
    display["Avg Monthly Load"] = display["avg_monthly_persisted_minutes"].apply(_human_duration)
    display["Total Load"] = display["total_duration_minutes"].apply(_human_duration)
    return display[[
        "location", "total_incidents", "Avg Event Length", "avg_duration_per_incident",
        "Avg Monthly Load", "avg_monthly_persisted_minutes", "avg_peak_db", "max_peak_db",
        "Total Load", "total_duration_minutes",
    ]].rename(columns={
        "location": "Location",
        "total_incidents": "Incidents",
        "avg_duration_per_incident": "Avg Event (min)",
        "avg_monthly_persisted_minutes": "Avg Monthly (min)",
        "avg_peak_db": "Avg Peak dB",
        "max_peak_db": "Max Peak dB",
        "total_duration_minutes": "Total (min)",
    })


# ── Data fetching ─────────────────────────────────────────────────────────────

def _fetch_monthly_chunk(client, view_name: str, location_id: str,
                          year: int, month: int) -> pd.DataFrame:
    _, last_day = calendar.monthrange(year, month)
    start = date(year, month, 1)
    end = date(year, month, last_day)

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
    """Return each continuous run in [min_db, max_db] lasting at least duration_minutes."""
    if df.empty or location_id not in df.columns:
        return []

    df = df.copy()
    df[location_id] = pd.to_numeric(df[location_id], errors="coerce")

    vals = df[location_id]
    in_range = vals.between(min_db, max_db, inclusive="both").fillna(False)
    group = (in_range != in_range.shift()).cumsum().where(in_range)

    incidents = []
    for _gid, chunk in df.groupby(group):
        if len(chunk) < duration_minutes:
            continue
        v = chunk[location_id].dropna()
        if v.empty:
            continue
        incidents.append({
            "duration": len(chunk),
            "peak_db": float(v.max()),
            "avg_db": float(v.mean()),
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
                .eq("location_id", loc_id)
                .eq("min_db", p["min_db"])
                .eq("max_db", p["max_db"])
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

    return _ensure_summary_metrics(pd.DataFrame(all_records))


def _compute_fresh(client, view_name: str, selected_locs: list[str],
                    preset_overrides: dict) -> pd.DataFrame:
    from location_presets import LOCATION_PRESETS

    months = _months_to_process()
    total_steps = len(selected_locs) * len(months)
    step = 0

    progress_bar = st.progress(0.0)
    status_text = st.empty()
    rows = []

    for loc_id in selected_locs:
        loc_name = LOCATION_PRESETS[loc_id]["name"]
        p = preset_overrides[loc_id]

        for yr, mo in months:
            label = _month_label(yr, mo)
            status_text.markdown(f"**{loc_name}** - {label} ({step + 1}/{total_steps})")

            try:
                chunk = _fetch_monthly_chunk(client, view_name, loc_id, yr, mo)
                incidents = _detect_incidents(chunk, loc_id, p["min_db"], p["max_db"], p["duration_minutes"])
            except Exception as exc:
                st.warning(f"Error fetching {loc_name} {label}: {exc}")
                incidents = []

            n = len(incidents)
            total_d = sum(i["duration"] for i in incidents)
            avg_dur = round(total_d / n, 1) if n > 0 else 0.0
            avg_peak = round(sum(i["peak_db"] for i in incidents) / n, 1) if n > 0 else 0.0
            max_peak = round(max(i["peak_db"] for i in incidents), 1) if n > 0 else 0.0

            rows.append({
                "location_id": loc_id,
                "location": loc_name,
                "month": date(yr, mo, 1),
                "month_label": label,
                "incident_count": n,
                "total_duration_minutes": total_d,
                "avg_duration_per_incident": avg_dur,
                "avg_peak_db": avg_peak,
                "max_peak_db": max_peak,
                "min_db": p["min_db"],
                "max_db": p["max_db"],
                "duration_minutes": p["duration_minutes"],
            })

            step += 1
            progress_bar.progress(step / total_steps)

    status_text.markdown("**Computation complete.**")
    progress_bar.progress(1.0)
    return _ensure_summary_metrics(pd.DataFrame(rows)) if rows else pd.DataFrame()


# ── Rendering ─────────────────────────────────────────────────────────────────

def _ordered_df(loc_df: pd.DataFrame) -> pd.DataFrame:
    """Build a complete 12-month spine so gaps show as 0, not missing bars."""
    month_window = [date(y, m, 1) for y, m in _months_to_process()]
    labels_in_order = [_month_label(d.year, d.month) for d in month_window]

    loc_df = _ensure_summary_metrics(loc_df)
    loc_df["month_label"] = pd.Categorical(
        loc_df["month_label"], categories=labels_in_order, ordered=True
    )

    spine = pd.DataFrame({"month": month_window, "month_label": labels_in_order})
    merged = spine.merge(loc_df, on=["month", "month_label"], how="left")

    numeric_cols = merged.select_dtypes(include="number").columns
    merged[numeric_cols] = merged[numeric_cols].fillna(0)
    merged["month_label"] = pd.Categorical(
        merged["month_label"], categories=labels_in_order, ordered=True
    )
    return merged.sort_values("month_label")


def _render_single_location(loc_id: str, df: pd.DataFrame) -> None:
    from location_presets import LOCATION_PRESETS

    loc_name = LOCATION_PRESETS[loc_id]["name"]
    preset = LOCATION_PRESETS[loc_id]

    loc_df = df[df["location_id"] == loc_id].copy() if "location_id" in df.columns else df.copy()
    if loc_df.empty:
        st.info(f"No data found for **{loc_name}**.")
        return

    loc_df = _ordered_df(loc_df)

    total_incidents = int(loc_df["incident_count"].sum())
    total_minutes = int(loc_df["total_duration_minutes"].sum())
    non_zero = loc_df[loc_df["incident_count"] > 0]
    overall_avg_dur = round(total_minutes / total_incidents, 1) if total_incidents > 0 else 0.0
    avg_peak = _safe_weighted_peak(loc_df)
    worst_month = (
        str(non_zero.loc[non_zero["avg_duration_per_incident"].idxmax(), "month_label"])
        if not non_zero.empty else "None"
    )

    st.markdown(f"#### {loc_name}")
    st.caption(
        f"Detection band: {preset['min_db']}-{preset['max_db']} dB, "
        f"sustained for {preset['duration_minutes']}+ minutes."
    )

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Incidents", f"{total_incidents:,}")
    m2.metric("Avg Event Length", _human_duration(overall_avg_dur))
    m3.metric("Total Noise Load", _human_duration(total_minutes))
    m4.metric("Avg Peak", f"{avg_peak:.1f} dB" if avg_peak else "0 dB")

    st.caption(f"Longest average event month: **{worst_month}**")

    c1, c2 = st.columns(2)
    with c1:
        _month_bar(loc_df, "incident_count", "Incidents by Month", "Incidents")
    with c2:
        _month_bar(loc_df, "avg_duration_per_incident", "Avg Event Length by Month", "Minutes")

    c3, c4 = st.columns(2)
    with c3:
        _month_bar(loc_df, "total_duration_minutes", "Total Noise Load by Month", "Minutes")
    with c4:
        _month_bar(loc_df, "avg_peak_db", "Average Peak dB by Month", "dB")

    st.markdown("**Monthly Breakdown**")
    display = loc_df[[
        "month_label", "incident_count", "avg_duration_per_incident",
        "total_duration_minutes", "avg_peak_db", "max_peak_db"
    ]].copy()
    display["Avg Event Length"] = display["avg_duration_per_incident"].apply(_human_duration)
    display["Total Noise Load"] = display["total_duration_minutes"].apply(_human_duration)
    display = display[[
        "month_label", "incident_count", "Avg Event Length", "avg_duration_per_incident",
        "Total Noise Load", "total_duration_minutes", "avg_peak_db", "max_peak_db"
    ]]
    display.columns = [
        "Month", "Incidents", "Avg Event Length", "Avg Event (min)",
        "Total Noise Load", "Total (min)", "Avg Peak dB", "Max Peak dB"
    ]
    st.dataframe(display, use_container_width=True, hide_index=True)


def _safe_weighted_peak(df: pd.DataFrame) -> float:
    total_incidents = df["incident_count"].sum()
    if total_incidents <= 0:
        return 0.0
    return round((df["avg_peak_db"] * df["incident_count"]).sum() / total_incidents, 1)


def _location_summary(df: pd.DataFrame) -> pd.DataFrame:
    df = _ensure_summary_metrics(df)
    df["weighted_peak_sum"] = df["avg_peak_db"] * df["incident_count"]
    summary = (
        df.groupby("location", as_index=False)
        .agg(
            total_incidents=("incident_count", "sum"),
            total_duration_minutes=("total_duration_minutes", "sum"),
            months_with_incidents=("incident_count", lambda s: int((s > 0).sum())),
            weighted_peak_sum=("weighted_peak_sum", "sum"),
            max_peak_db=("max_peak_db", "max"),
        )
    )
    summary["avg_duration_per_incident"] = summary.apply(
        lambda r: round(r["total_duration_minutes"] / r["total_incidents"], 1)
        if r["total_incidents"] > 0 else 0.0,
        axis=1,
    )
    summary["avg_monthly_persisted_minutes"] = (
        summary["total_duration_minutes"] / len(_months_to_process())
    ).round(1)
    summary["avg_active_month_minutes"] = summary.apply(
        lambda r: round(r["total_duration_minutes"] / r["months_with_incidents"], 1)
        if r["months_with_incidents"] > 0 else 0.0,
        axis=1,
    )
    summary["avg_peak_db"] = summary.apply(
        lambda r: round(r["weighted_peak_sum"] / r["total_incidents"], 1)
        if r["total_incidents"] > 0 else 0.0,
        axis=1,
    )
    return summary.sort_values("total_incidents", ascending=False)


def _render_heatmap(df: pd.DataFrame, selected_locs: list[str]) -> None:
    from location_presets import LOCATION_PRESETS

    st.markdown("**Monthly Pattern Heatmap**")
    metric_label = st.radio(
        "Show",
        options=["Incidents", "Avg Event Length", "Total Noise Load", "Avg Peak dB"],
        horizontal=True,
        key="yearly_heatmap_metric",
    )
    metric_map = {
        "Incidents": ("incident_count", "sum", "Incidents"),
        "Avg Event Length": ("avg_duration_per_incident", "mean", "Minutes"),
        "Total Noise Load": ("total_duration_minutes", "sum", "Minutes"),
        "Avg Peak dB": ("avg_peak_db", "mean", "dB"),
    }
    metric, aggfunc, legend_title = metric_map[metric_label]

    location_order = [LOCATION_PRESETS[loc]["name"] for loc in selected_locs if loc in LOCATION_PRESETS]
    heatmap = df.pivot_table(
        index="location",
        columns="month_label",
        values=metric,
        aggfunc=aggfunc,
        fill_value=0,
    ).reindex(index=location_order, columns=_month_order(), fill_value=0)

    plot_df = heatmap.reset_index().melt(
        id_vars="location", var_name="month_label", value_name="value"
    )
    chart = (
        alt.Chart(plot_df)
        .mark_rect(stroke="white", strokeWidth=1)
        .encode(
            x=alt.X("month_label:N", sort=_month_order(), title=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y("location:N", sort=location_order, title=None, axis=alt.Axis(labelLimit=360)),
            color=alt.Color("value:Q", title=legend_title, scale=alt.Scale(scheme="blues")),
            tooltip=[
                alt.Tooltip("location:N", title="Location"),
                alt.Tooltip("month_label:N", title="Month"),
                alt.Tooltip("value:Q", title=legend_title, format=",.1f"),
            ],
        )
        .properties(height=max(280, len(location_order) * 34))
    )
    st.altair_chart(chart, use_container_width=True)


def _render_multi_location(selected_locs: list[str], df: pd.DataFrame) -> None:
    from location_presets import LOCATION_PRESETS

    st.markdown("#### Cross-Location Comparison")

    df = _ensure_summary_metrics(df)
    if "location" not in df.columns:
        df["location"] = df["location_id"].map(
            lambda x: LOCATION_PRESETS.get(x, {}).get("name", x)
        )

    loc_summary = _location_summary(df)
    if loc_summary.empty:
        st.info("No data available for the selected locations.")
        return

    busiest = loc_summary.iloc[0]
    longest = loc_summary.sort_values("avg_duration_per_incident", ascending=False).iloc[0]
    loudest = loc_summary.sort_values("avg_peak_db", ascending=False).iloc[0]

    k1, k2, k3 = st.columns(3)
    k1.metric("Most Incidents", busiest["location"], f"{int(busiest['total_incidents']):,} incidents")
    k2.metric("Longest Typical Event", longest["location"], _human_duration(longest["avg_duration_per_incident"]))
    k3.metric("Highest Avg Peak", loudest["location"], f"{loudest['avg_peak_db']:.1f} dB")

    st.caption(
        "Read this as three different measures: incident count is frequency, average event length is persistence, "
        "and peak dB is loudness. Total minutes is kept as noise load, but it is not the main comparison chart."
    )

    overview_tab, pattern_tab, table_tab, drilldown_tab = st.tabs([
        "Overview", "Monthly Pattern", "Details Table", "Location Drilldown"
    ])

    with overview_tab:
        c1, c2 = st.columns(2)
        with c1:
            _ranked_bar(loc_summary, "total_incidents", "Frequency: Total Incidents", "Incidents")
        with c2:
            _ranked_bar(loc_summary, "avg_duration_per_incident", "Persistence: Avg Event Length", "Minutes", "#2ca02c")

        c3, c4 = st.columns(2)
        with c3:
            _ranked_bar(loc_summary, "avg_monthly_persisted_minutes", "Noise Load: Avg Minutes per Month", "Minutes", "#9467bd")
        with c4:
            _ranked_bar(loc_summary, "avg_peak_db", "Loudness: Avg Peak dB", "dB", "#d62728")

    with pattern_tab:
        _render_heatmap(df, selected_locs)

    with table_tab:
        st.markdown("**Ranked Location Summary**")
        st.dataframe(_format_summary_table(loc_summary), use_container_width=True, hide_index=True)

    with drilldown_tab:
        drilldown_loc = st.selectbox(
            "Select a location",
            options=selected_locs,
            format_func=lambda x: LOCATION_PRESETS[x]["name"],
            key="yearly_drilldown_loc",
        )
        _render_single_location(drilldown_loc, df)


def _render_results(summary_df: pd.DataFrame,
                     selected_locs: list[str],
                     range_label: str) -> None:
    st.markdown("---")
    st.markdown(f"### Results - {range_label}")

    summary_df = _ensure_summary_metrics(summary_df)
    if len(selected_locs) == 1:
        _render_single_location(selected_locs[0], summary_df)
    else:
        _render_multi_location(selected_locs, summary_df)

    st.markdown("---")
    st.markdown("### Export")
    export_cols = [c for c in summary_df.columns if c != "location_id"]
    csv = summary_df[export_cols].to_csv(index=False)
    st.download_button(
        label="Download Summary CSV",
        data=csv,
        file_name=f"yearly_noise_summary_{range_label.replace(' ', '_')}.csv",
        mime="text/csv",
        use_container_width=True,
    )


# ── Main entry point ──────────────────────────────────────────────────────────

def show_yearly_analysis_tab(supabase_client, view_name: str) -> None:
    from location_presets import LOCATION_PRESETS

    st.markdown("### Yearly Persisted Noise Analysis")
    st.caption(
        "Compare sustained noise across locations by frequency, event length, monthly load, and peak dB."
    )

    all_loc_ids = list(LOCATION_PRESETS.keys())
    selected_locs = st.multiselect(
        "Locations",
        options=all_loc_ids,
        default=[all_loc_ids[0]],
        format_func=lambda x: LOCATION_PRESETS[x]["name"],
        help="Select one or more locations.",
        key="yearly_location_picker",
    )

    today_month = date.today().replace(day=1)
    start_month = (pd.Timestamp(today_month) - pd.DateOffset(months=11)).date()
    range_label = f"{start_month.strftime('%b %Y')} -> {today_month.strftime('%b %Y')}"
    st.info(f"Showing rolling 12-month window: **{range_label}**")

    if not selected_locs:
        st.warning("Please select at least one location to continue.")
        return

    st.markdown("#### Detection Thresholds per Location")
    st.caption(
        "An incident is one unbroken run of readings inside the dB band for at least the selected duration."
    )

    preset_overrides: dict[str, dict] = {}

    for i in range(0, len(selected_locs), 2):
        cols = st.columns(2)
        for j in range(2):
            if i + j >= len(selected_locs):
                break
            loc_id = selected_locs[i + j]
            default = LOCATION_PRESETS[loc_id]

            with cols[j]:
                with st.expander(default["name"], expanded=False):
                    st.caption(default.get("notes", ""))
                    min_db = st.number_input("Min dB", value=float(default["min_db"]),
                                              min_value=0.0, max_value=200.0, step=1.0,
                                              key=f"yr_min_{loc_id}")
                    max_db = st.number_input("Max dB", value=float(default["max_db"]),
                                              min_value=0.0, max_value=200.0, step=1.0,
                                              key=f"yr_max_{loc_id}")
                    dur = st.number_input("Min Duration (min)", value=int(default["duration_minutes"]),
                                          min_value=1, max_value=60, step=1,
                                          key=f"yr_dur_{loc_id}")
                    preset_overrides[loc_id] = {"min_db": min_db, "max_db": max_db, "duration_minutes": dur}

    for loc_id in selected_locs:
        if loc_id not in preset_overrides:
            d = LOCATION_PRESETS[loc_id]
            preset_overrides[loc_id] = {
                "min_db": d["min_db"],
                "max_db": d["max_db"],
                "duration_minutes": d["duration_minutes"],
            }

    st.markdown("---")
    st.markdown(
        "**Load Pre-computed** reads from `monthly_persisted_summary` instantly. "
        "**Compute Now** fetches raw data month-by-month and is slower."
    )

    col_fast, col_slow = st.columns(2)
    load_btn = col_fast.button("Load Pre-computed Data", use_container_width=True, type="primary")
    compute_btn = col_slow.button("Compute Now (Slow)", use_container_width=True)

    cache_key = f"{sorted(selected_locs)}_{start_month}_{today_month}_{str(preset_overrides)}"

    if load_btn:
        with st.spinner("Loading pre-computed monthly summaries from Supabase..."):
            summary_df = _load_precomputed(supabase_client, selected_locs,
                                            start_month, today_month, preset_overrides)
        if summary_df.empty:
            st.warning(
                "No pre-computed summaries found for these exact settings. "
                "Run the ETL job first, or click **Compute Now**."
            )
        else:
            st.session_state.update({
                "yr_summary_df": summary_df,
                "yr_cache_key": cache_key,
                "yr_locs": selected_locs,
                "yr_range_label": range_label,
            })

    if compute_btn:
        summary_df = _compute_fresh(supabase_client, view_name, selected_locs, preset_overrides)
        if not summary_df.empty:
            st.session_state.update({
                "yr_summary_df": summary_df,
                "yr_cache_key": cache_key,
                "yr_locs": selected_locs,
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
