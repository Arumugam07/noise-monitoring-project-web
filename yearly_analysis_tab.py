"""
yearly_analysis_tab.py
----------------------
Streamlit tab: Yearly Persisted Noise Analysis

Workflow:
  1. User picks locations + year + reviews/adjusts per-location presets.
  2. "Load Pre-computed" → fast read from monthly_persisted_summary table (ETL fills this).
  3. "Compute Now"       → fetches raw data month-by-month with a progress bar,
                           computes incidents in-memory, displays results immediately.
  4. Results show per-location or cross-location comparison charts + tables.

Import into app.py and call show_yearly_analysis_tab() inside a st.tabs() block.
"""

import calendar
from datetime import date

import pandas as pd
import streamlit as st

# ── Import shared helpers from your main app ────────────────────────────────
# get_client() is already cached in app.py; importing it here reuses the cache.
# DEFAULT_VIEW is a module-level constant in app.py.
# We import lazily inside functions to avoid circular-import issues at load time.

READINGS_PER_DAY = 1440

MONTH_ORDER = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


# ── Data helpers ─────────────────────────────────────────────────────────────

def _fetch_monthly_chunk(client, view_name: str, location_id: str,
                          year: int, month: int) -> pd.DataFrame:
    """Fetch one month of minute-level data for a single location column."""
    _, last_day = calendar.monthrange(year, month)
    start = date(year, month, 1)
    end = date(year, month, last_day)

    all_data = []
    offset = 0
    batch_size = 1000

    while True:
        resp = (
            client.table(view_name)
            .select(f"Date,Time,{location_id}")
            .gte("Date", str(start))
            .lte("Date", str(end))
            .order("Date")
            .order("Time")
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
    """Return list of incident dicts for one location/month chunk."""
    if df.empty or location_id not in df.columns:
        return []

    df = df.copy()
    df[location_id] = pd.to_numeric(df[location_id], errors="coerce")

    vals = df[location_id]
    in_range = vals.between(min_db, max_db, inclusive="both").fillna(False)
    group = (in_range != in_range.shift()).cumsum()
    group = group.where(in_range)

    incidents = []
    for _gid, idx_group in df.groupby(group):
        if len(idx_group) < duration_minutes:
            continue
        incident_vals = idx_group[location_id].dropna()
        if incident_vals.empty:
            continue
        incidents.append({
            "duration": len(idx_group),
            "peak_db":  float(incident_vals.max()),
            "avg_db":   float(incident_vals.mean()),
        })
    return incidents


def _load_precomputed(client, location_ids: list[str], year: int,
                       preset_overrides: dict) -> pd.DataFrame:
    """
    Pull already-computed monthly summaries from Supabase.
    Matches on location_id + month + exact min_db/max_db/duration_minutes
    so changing presets forces a fresh compute.
    """
    start = str(date(year, 1, 1))
    end   = str(date(year, 12, 31))
    all_records = []

    for loc_id in location_ids:
        p = preset_overrides[loc_id]
        try:
            resp = (
                client.table("monthly_persisted_summary")
                .select("*")
                .eq("location_id",       loc_id)
                .eq("min_db",            p["min_db"])
                .eq("max_db",            p["max_db"])
                .eq("duration_minutes",  p["duration_minutes"])
                .gte("month", start)
                .lte("month", end)
                .execute()
            )
            all_records.extend(resp.data or [])
        except Exception as exc:
            st.warning(f"Could not load pre-computed data for {loc_id}: {exc}")

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df["month"] = pd.to_datetime(df["month"]).dt.date
    return df


def _months_to_process(year: int) -> list[tuple[int, int]]:
    """Return list of (year, month) tuples from Jan of `year` up to today."""
    today = date.today()
    result = []
    for m in range(1, 13):
        if year < today.year or (year == today.year and m <= today.month):
            result.append((year, m))
    return result


def _compute_fresh(client, view_name: str, selected_locs: list[str],
                    year: int, preset_overrides: dict) -> pd.DataFrame:
    """
    Fetch raw data month by month and compute incidents.
    Saves nothing to Supabase (anon key is read-only).
    The ETL job (compute_monthly_summary.py) handles persistence.
    """
    from location_presets import LOCATION_PRESETS

    months = _months_to_process(year)
    total_steps = len(selected_locs) * len(months)
    step = 0

    progress_bar = st.progress(0.0)
    status_text  = st.empty()
    rows = []

    for loc_id in selected_locs:
        loc_name = LOCATION_PRESETS[loc_id]["name"]
        p = preset_overrides[loc_id]

        for yr, mo in months:
            month_label = date(yr, mo, 1).strftime("%B %Y")
            status_text.markdown(
                f"⏳ **{loc_name}** — {month_label} "
                f"({step + 1}/{total_steps})"
            )

            try:
                chunk = _fetch_monthly_chunk(client, view_name, loc_id, yr, mo)
                incidents = _detect_incidents(
                    chunk, loc_id, p["min_db"], p["max_db"], p["duration_minutes"]
                )
            except Exception as exc:
                st.warning(f"Error fetching {loc_name} {month_label}: {exc}")
                incidents = []

            rows.append({
                "location_id":             loc_id,
                "location":                loc_name,
                "month":                   date(yr, mo, 1),
                "month_label":             date(yr, mo, 1).strftime("%b"),
                "incident_count":          len(incidents),
                "total_duration_minutes":  sum(i["duration"] for i in incidents),
                "avg_peak_db":             round(
                    sum(i["peak_db"] for i in incidents) / len(incidents), 1
                ) if incidents else 0.0,
                "max_peak_db":             round(
                    max(i["peak_db"] for i in incidents), 1
                ) if incidents else 0.0,
                "min_db":                  p["min_db"],
                "max_db":                  p["max_db"],
                "duration_minutes":        p["duration_minutes"],
            })

            step += 1
            progress_bar.progress(step / total_steps)

    status_text.markdown("✅ **Computation complete!**")
    progress_bar.progress(1.0)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ── Result rendering ──────────────────────────────────────────────────────────

def _render_single_location(loc_id: str, df: pd.DataFrame) -> None:
    from location_presets import LOCATION_PRESETS

    loc_name = LOCATION_PRESETS[loc_id]["name"]
    preset   = LOCATION_PRESETS[loc_id]

    loc_df = df[df["location_id"] == loc_id].copy() if "location_id" in df.columns else df.copy()

    if loc_df.empty:
        st.info(f"No data found for **{loc_name}**.")
        return

    # Ensure month_label column
    loc_df["month_label"] = pd.to_datetime(loc_df["month"]).dt.strftime("%b")
    loc_df = (
        loc_df.set_index("month_label")
        .reindex(MONTH_ORDER)
        .reset_index()
        .fillna(0)
    )

    st.markdown(f"#### 📍 {loc_name}")
    st.caption(
        f"Detection band: **{preset['min_db']}–{preset['max_db']} dB** "
        f"sustained for **{preset['duration_minutes']}+ minutes**"
    )

    total_incidents = int(loc_df["incident_count"].sum())
    total_minutes   = int(loc_df["total_duration_minutes"].sum())
    non_zero        = loc_df[loc_df["incident_count"] > 0]
    worst_month     = (
        non_zero.loc[non_zero["incident_count"].idxmax(), "month_label"]
        if not non_zero.empty else "—"
    )
    avg_peak = (
        round(loc_df[loc_df["avg_peak_db"] > 0]["avg_peak_db"].mean(), 1)
        if (loc_df["avg_peak_db"] > 0).any() else 0.0
    )

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Incidents",        f"{total_incidents:,}")
    m2.metric("Total Persisted Minutes", f"{total_minutes:,}")
    m3.metric("Worst Month",            worst_month)
    m4.metric("Avg Peak dB",            f"{avg_peak}")

    st.markdown("**Incidents per Month**")
    st.bar_chart(loc_df.set_index("month_label")["incident_count"])

    st.markdown("**Total Persisted Duration per Month (minutes)**")
    st.bar_chart(loc_df.set_index("month_label")["total_duration_minutes"])

    if "avg_peak_db" in loc_df.columns:
        st.markdown("**Average Peak dB per Month**")
        st.bar_chart(loc_df.set_index("month_label")["avg_peak_db"])

    st.markdown("**Monthly Breakdown**")
    display = loc_df[["month_label", "incident_count",
                       "total_duration_minutes", "avg_peak_db", "max_peak_db"]].copy()
    display.columns = ["Month", "Incidents", "Total Duration (min)",
                        "Avg Peak dB", "Max Peak dB"]
    st.dataframe(display, use_container_width=True, hide_index=True)


def _render_multi_location(selected_locs: list[str], df: pd.DataFrame) -> None:
    from location_presets import LOCATION_PRESETS

    st.markdown("#### 🗺️ Cross-Location Comparison")

    # Attach friendly name
    if "location" not in df.columns:
        df = df.copy()
        df["location"] = df["location_id"].map(
            lambda x: LOCATION_PRESETS.get(x, {}).get("name", x)
        )

    # ── Per-location totals ──────────────────────────────────────────────────
    loc_summary = (
        df.groupby("location")
        .agg(
            total_incidents       =("incident_count",        "sum"),
            total_duration_minutes=("total_duration_minutes","sum"),
            avg_peak_db           =("avg_peak_db",           "mean"),
        )
        .reset_index()
        .sort_values("total_incidents", ascending=False)
    )
    loc_summary["avg_peak_db"] = loc_summary["avg_peak_db"].round(1)

    st.markdown("**Total Incidents per Location (full year)**")
    st.bar_chart(loc_summary.set_index("location")["total_incidents"])

    st.markdown("**Total Persisted Minutes per Location (full year)**")
    st.bar_chart(loc_summary.set_index("location")["total_duration_minutes"])

    # ── Monthly heatmap: rows = months, cols = locations ────────────────────
    st.markdown("**Monthly Incident Heatmap (rows = month, cols = location)**")
    df["month_label"] = pd.to_datetime(df["month"]).dt.strftime("%b")

    heatmap = df.pivot_table(
        index="month_label",
        columns="location",
        values="incident_count",
        aggfunc="sum",
        fill_value=0,
    )
    heatmap = heatmap.reindex([m for m in MONTH_ORDER if m in heatmap.index])
    st.dataframe(heatmap, use_container_width=True)

    # ── Summary table ────────────────────────────────────────────────────────
    st.markdown("**Location Summary Table**")
    disp = loc_summary[["location", "total_incidents",
                          "total_duration_minutes", "avg_peak_db"]].copy()
    disp.columns = ["Location", "Total Incidents",
                     "Total Persisted Minutes", "Avg Peak dB"]
    st.dataframe(disp, use_container_width=True, hide_index=True)

    # ── Per-location drilldown ───────────────────────────────────────────────
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
                     year: int) -> None:
    st.markdown("---")
    st.markdown(f"### 📊 Results — {year}")

    if len(selected_locs) == 1:
        _render_single_location(selected_locs[0], summary_df)
    else:
        _render_multi_location(selected_locs, summary_df)

    # ── Export ───────────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### 📥 Export")
    export_cols = [c for c in summary_df.columns
                   if c not in ("location_id",)]
    csv = summary_df[export_cols].to_csv(index=False)
    st.download_button(
        label="📄 Download Summary CSV",
        data=csv,
        file_name=f"yearly_noise_summary_{year}.csv",
        mime="text/csv",
        use_container_width=True,
    )


# ── Main entry point (called from app.py) ────────────────────────────────────

def show_yearly_analysis_tab(supabase_client, view_name: str) -> None:
    """
    Full UI for the Yearly Persisted Noise Analysis tab.
    Call this inside a `with tab:` block in app.py.
    """
    from location_presets import LOCATION_PRESETS

    st.markdown("### 📅 Yearly Persisted Noise Analysis")
    st.caption(
        "Analyze how often noise stays in a defined dB band for a sustained period "
        "across an entire year, loaded month-by-month to avoid Supabase timeouts."
    )

    # ── Location + Year selectors ─────────────────────────────────────────────
    col_loc, col_year = st.columns([3, 1])

    with col_loc:
        all_loc_ids = list(LOCATION_PRESETS.keys())
        selected_locs = st.multiselect(
            "📍 Locations",
            options=all_loc_ids,
            default=[all_loc_ids[0]],
            format_func=lambda x: LOCATION_PRESETS[x]["name"],
            help="Select one or more locations. Each uses its own threshold preset.",
            key="yearly_location_picker",
        )

    with col_year:
        today = date.today()
        year_options = list(range(2025, today.year + 1))
        year = st.selectbox(
            "📆 Year",
            options=year_options,
            index=len(year_options) - 1,
            key="yearly_year_picker",
        )

    if not selected_locs:
        st.warning("Please select at least one location to continue.")
        return

    # ── Per-location preset editor ────────────────────────────────────────────
    st.markdown("#### ⚙️ Detection Thresholds per Location")
    st.caption(
        "Pre-filled from `location_presets.py`. Adjust here for one-off analysis — "
        "permanent changes should be made in the file itself."
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
                with st.expander(f"📍 {default['name']}", expanded=False):
                    st.caption(default.get("notes", ""))
                    min_db = st.number_input(
                        "Min dB", value=float(default["min_db"]),
                        min_value=0.0, max_value=200.0, step=1.0,
                        key=f"yr_min_{loc_id}",
                    )
                    max_db = st.number_input(
                        "Max dB", value=float(default["max_db"]),
                        min_value=0.0, max_value=200.0, step=1.0,
                        key=f"yr_max_{loc_id}",
                    )
                    dur = st.number_input(
                        "Min Duration (min)", value=int(default["duration_minutes"]),
                        min_value=1, max_value=60, step=1,
                        key=f"yr_dur_{loc_id}",
                    )
                    preset_overrides[loc_id] = {
                        "min_db": min_db, "max_db": max_db, "duration_minutes": dur
                    }

    # Backfill any location whose expander wasn't opened (uses default preset)
    for loc_id in selected_locs:
        if loc_id not in preset_overrides:
            d = LOCATION_PRESETS[loc_id]
            preset_overrides[loc_id] = {
                "min_db": d["min_db"],
                "max_db": d["max_db"],
                "duration_minutes": d["duration_minutes"],
            }

    # ── Action buttons ────────────────────────────────────────────────────────
    st.markdown("---")
    st.markdown(
        "**⚡ Load Pre-computed** reads from the `monthly_persisted_summary` table "
        "filled by the nightly ETL job — instant. "
        "**🔄 Compute Now** fetches raw data month-by-month — takes ~1-3 minutes "
        "depending on date range and number of locations."
    )

    col_fast, col_slow = st.columns(2)
    load_btn    = col_fast.button("⚡ Load Pre-computed Data",  use_container_width=True, type="primary")
    compute_btn = col_slow.button("🔄 Compute Now (Slow)",      use_container_width=True)

    # Cache key: invalidate when user changes selections
    cache_key = f"{sorted(selected_locs)}_{year}_{str(preset_overrides)}"

    # ── Load pre-computed ─────────────────────────────────────────────────────
    if load_btn:
        with st.spinner("Loading pre-computed monthly summaries from Supabase…"):
            summary_df = _load_precomputed(
                supabase_client, selected_locs, year, preset_overrides
            )

        if summary_df.empty:
            st.warning(
                "No pre-computed summaries found for these exact settings. "
                "Either run the ETL job (`etl/compute_monthly_summary.py`) first, "
                "or click **🔄 Compute Now** to calculate on-the-fly."
            )
        else:
            st.session_state["yr_summary_df"]  = summary_df
            st.session_state["yr_cache_key"]   = cache_key
            st.session_state["yr_locs"]        = selected_locs
            st.session_state["yr_year"]        = year

    # ── Compute fresh ─────────────────────────────────────────────────────────
    if compute_btn:
        summary_df = _compute_fresh(
            supabase_client, view_name, selected_locs, year, preset_overrides
        )
        if not summary_df.empty:
            st.session_state["yr_summary_df"] = summary_df
            st.session_state["yr_cache_key"]  = cache_key
            st.session_state["yr_locs"]       = selected_locs
            st.session_state["yr_year"]       = year

    # ── Render cached results (survive Streamlit reruns) ─────────────────────
    if (
        "yr_summary_df" in st.session_state
        and st.session_state.get("yr_cache_key") == cache_key
    ):
        _render_results(
            st.session_state["yr_summary_df"],
            st.session_state["yr_locs"],
            st.session_state["yr_year"],
        )
