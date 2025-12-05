#!/usr/bin/env python3
"""
Streamlit app: Simple login + interactive table over Supabase wide view.

- Login gate (single username/password)
- Filters: Date range, location columns, numeric range
- Pagination and vertical scrolling

Assumptions:
- A wide view exists in Supabase: `public.wide_view` (or materialized `wide_view_mv`).
- Columns: Date (date), Time (time), and one column per location_id as strings.
- We map location IDs to English names for display.
"""

import os
import io
from datetime import date, timedelta

import pandas as pd
import streamlit as st
from supabase import create_client
from dotenv import load_dotenv

# Map location IDs → friendly names for column display
LOCATION_ID_TO_NAME = {
    "15490": "Singapore Sports School",
    "16034": "BLK 120 Serangoon North Ave 1",
    "16041": "BLK 838 Hougang Central",
    "14542": "BLK 558 Jurong West Street 42",
    "15725": "Jurong Safra, Block C",
    "16032": "AMA KENG SITE",
    "16045": "BLK 19 Balam Road",
    "15820": "Norcom II Tower 4",
    "15821": "Blk 444 Choa Chu Kang Avenue 4",
    "15999": "BLK 654B Punggol Drive",
    "16026": "BLK 132B Tengah Garden Avenue",
    "16004": "BLK 206A Punggol Place",
    "16005": "Woodlands 11",
}

DEFAULT_VIEW = os.getenv("SUPABASE_WIDE_VIEW", "wide_view_mv")
PAGE_SIZE = 200


def get_client():
    """Create Supabase client from env or Streamlit secrets."""
    load_dotenv()

    # Prefer Streamlit secrets if present
    url = os.getenv("SUPABASE_URL") or st.secrets.get("SUPABASE_URL")
    key = os.getenv("SUPABASE_ANON_KEY") or st.secrets.get("SUPABASE_ANON_KEY")

    if not url or not key:
        raise RuntimeError(
            "SUPABASE_URL or SUPABASE_ANON_KEY not set. "
            "Set them as environment variables or in .streamlit/secrets.toml."
        )

    return create_client(url, key)


def fetch_page(page: int, page_size: int, start_date=None, end_date=None) -> pd.DataFrame:
    """Fetch data with pagination and optional date filtering."""
    supabase = get_client()
    offset = page * page_size

    try:
        # Query the wide view with date filters if provided
        query = supabase.table(DEFAULT_VIEW).select("*")
        
        # Add date filters to reduce data volume
        if start_date:
            query = query.gte("Date", str(start_date))
        if end_date:
            query = query.lte("Date", str(end_date))
        
        # Order and paginate
        query = query.order("Date").order("Time").range(offset, offset + page_size - 1)
        
        resp = query.execute()
        df = pd.DataFrame(resp.data or [])
        
        return df
        
    except Exception as e:
        st.error(f"Error fetching from {DEFAULT_VIEW}: {e}")
        # Fallback: try without filters
        try:
            resp = supabase.table(DEFAULT_VIEW).select("*").limit(page_size).execute()
            df = pd.DataFrame(resp.data or [])
            return df
        except Exception as e2:
            st.error(f"Fallback also failed: {e2}")
            return pd.DataFrame()


def filter_frame(df: pd.DataFrame, start_date, end_date, location_ids, vmin, vmax):
    if df.empty:
        return df

    df = df.copy()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

    # --- Date filter (only if not already filtered in query) ---
    if start_date is not None and end_date is not None:
        df = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]

    # --- Keep selected location columns ---
    id_cols = [c for c in df.columns if c not in ("Date", "Time")]
    keep_ids = [lid for lid in id_cols if lid in location_ids]

    if keep_ids:
        df = df[["Date", "Time"] + keep_ids]
    else:
        df = df[["Date", "Time"]]

    for col in keep_ids:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- Numeric filters ---
    if keep_ids and (vmin is not None or vmax is not None):
        for col in keep_ids:
            if vmin is not None:
                df = df[(df[col].isna()) | (df[col] >= vmin)]
            if vmax is not None:
                df = df[(df[col].isna()) | (df[col] <= vmax)]

    rename = {lid: LOCATION_ID_TO_NAME.get(lid, lid) for lid in keep_ids}
    return df.rename(columns=rename)


def login_gate() -> bool:
    st.sidebar.header("🔐 Authentication")
    user = st.sidebar.text_input("Username", placeholder="Enter username")
    pwd = st.sidebar.text_input("Password", type="password", placeholder="Enter password")

    if st.sidebar.button("Sign in", type="primary", use_container_width=True):
        # Get credentials from environment variables or secrets
        valid_user = (
            os.getenv("APP_USERNAME")
            or st.secrets.get("APP_USERNAME")
            or "admin"
        )
        valid_pwd = (
            os.getenv("APP_PASSWORD")
            or st.secrets.get("APP_PASSWORD")
            or "changeme"
        )

        if user == valid_user and pwd == valid_pwd:
            st.session_state["auth"] = True
            st.sidebar.success("✅ Login successful!")
            st.rerun()
        else:
            st.sidebar.error("❌ Invalid credentials")
            return False

    return st.session_state.get("auth", False)


def main():
    st.set_page_config(page_title="Noise Monitoring System", layout="wide", page_icon="🔊")
    st.title("🔊 Noise Monitoring System")
    st.caption("Real-time noise level monitoring across multiple locations in Singapore")

    if not login_gate():
        st.info("👆 Please log in using the sidebar to continue.")
        st.stop()

    # Logout button
    if st.sidebar.button("🚪 Logout", use_container_width=True):
        st.session_state["auth"] = False
        st.rerun()

    # Info section in sidebar
    with st.sidebar.expander("ℹ️ About", expanded=False):
        st.markdown(
            """
        **Noise Monitoring System**
        
        This dashboard displays noise level readings (in decibels) collected every minute from monitoring stations across Singapore.
        
        **Data Structure:**
        - Readings are collected every minute
        - Data is organized by date, time, and location
        - Values represent noise levels in dB
        
        **Features:**
        - Filter by date range and locations
        - Filter by noise level range
        - Export data as CSV or Excel
        - Pagination for large datasets
        """
        )

    st.sidebar.header("🔍 Filters")
    st.sidebar.markdown("---")

    # Safe default date range: last 7 days
    today = date.today()
    default_start = today - timedelta(days=7)

    date_selection = st.sidebar.date_input(
        "📅 Date Range",
        value=(default_start, today)
    )
    
    # Normalize
    if isinstance(date_selection, (list, tuple)):
        if len(date_selection) == 2:
            start_date, end_date = date_selection
        elif len(date_selection) == 1:
            start_date = end_date = date_selection[0]
        else:
            start_date = end_date = None
    elif isinstance(date_selection, date):
        start_date = end_date = date_selection
    else:
        start_date = end_date = None

    st.sidebar.markdown("---")

    all_ids = list(LOCATION_ID_TO_NAME.keys())
    selected_ids = st.sidebar.multiselect(
        "📍 Locations",
        options=all_ids,
        default=all_ids,
        format_func=lambda x: LOCATION_ID_TO_NAME.get(x, x),
        help="Select one or more monitoring locations",
    )

    st.sidebar.markdown("---")

    st.sidebar.subheader("📊 Value Range (dB)")
    use_min = st.sidebar.checkbox("Filter by minimum value", value=False)
    vmin = None
    if use_min:
        vmin = st.sidebar.number_input(
            "Minimum Value (dB)",
            value=40.0,
            help="Filter readings above this value",
        )

    use_max = st.sidebar.checkbox("Filter by maximum value", value=False)
    vmax = None
    if use_max:
        vmax = st.sidebar.number_input(
            "Maximum Value (dB)",
            value=100.0,
            help="Filter readings below this value",
        )

    st.sidebar.markdown("---")

    st.sidebar.subheader("📄 Pagination")
    page = st.sidebar.number_input(
        "Page Number",
        min_value=0,
        value=0,
        step=1,
        help=f"Navigate through pages (each page shows {PAGE_SIZE} rows)",
    )

    if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
        st.rerun()

    try:
        with st.spinner("Loading data from database..."):
            df = fetch_page(page, PAGE_SIZE, start_date, end_date)
            filtered = filter_frame(df, start_date, end_date, selected_ids, vmin, vmax)

        if not filtered.empty:
            # Display summary statistics
            st.markdown("### 📊 Summary Statistics")
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Total Records", len(filtered))

            # Calculate statistics for numeric columns (location columns)
            numeric_cols = [c for c in filtered.columns if c not in ("Date", "Time")]
            if numeric_cols:
                all_values = []
                for col in numeric_cols:
                    all_values.extend(filtered[col].dropna().tolist())

                if all_values:
                    avg_val = sum(all_values) / len(all_values)
                    with col2:
                        st.metric("Average Reading", f"{avg_val:.2f} dB")
                    with col3:
                        st.metric("Min Reading", f"{min(all_values):.2f} dB")
                    with col4:
                        st.metric("Max Reading", f"{max(all_values):.2f} dB")

            st.divider()

            # Display the data table with enhanced formatting
            st.markdown("### 📋 Data Table")
            st.caption(
                f"Showing {len(filtered)} rows (page {page + 1}, page size {PAGE_SIZE}). "
                "Use filters in the sidebar to refine results."
            )

            display_df = filtered.copy()
            if "Date" in display_df.columns:
                display_df["Date"] = pd.to_datetime(display_df["Date"]).dt.strftime(
                    "%Y-%m-%d"
                )
            if "Time" in display_df.columns:
                display_df["Time"] = display_df["Time"].astype(str)

            format_dict = {
                col: "{:.2f}" for col in numeric_cols if col in display_df.columns
            }
            if format_dict:
                styled_df = display_df.style.format(format_dict, na_rep="N/A")
                st.dataframe(
                    styled_df,
                    use_container_width=True,
                    height=600,
                    hide_index=True,
                )
            else:
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    height=600,
                    hide_index=True,
                )

            # Enhanced download functionality
            st.divider()
            st.markdown("### 📥 Export Data")

            col_dl1, col_dl2 = st.columns(2)

            with col_dl1:
                csv = filtered.to_csv(index=False)
                timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                filename = f"noise_readings_{timestamp}.csv"
                st.download_button(
                    label="📥 Download Current View (CSV)",
                    data=csv,
                    file_name=filename,
                    mime="text/csv",
                    use_container_width=True,
                    help="Download the currently filtered and displayed data",
                )

            with col_dl2:
                try:
                    excel_buffer = io.BytesIO()
                    filtered.to_excel(excel_buffer, index=False, engine="openpyxl")
                    excel_buffer.seek(0)
                    st.download_button(
                        label="📊 Download as Excel",
                        data=excel_buffer,
                        file_name=f"noise_readings_{timestamp}.xlsx",
                        mime=(
                            "application/"
                            "vnd.openxmlformats-officedocument."
                            "spreadsheetml.sheet"
                        ),
                        use_container_width=True,
                        help="Download data in Excel format (.xlsx)",
                    )
                except Exception:
                    st.info("💡 Excel export temporarily unavailable")
        else:
            st.warning("⚠️ No data found matching your filters.")
            st.info(
                """
            💡 Try adjusting:
            - **Date Range**: Select a wider date range
            - **Locations**: Select different or all locations
            - **Value Range**: Adjust or remove min/max value filters
            - **Page Number**: Try page 0 or check if data exists
            """
            )
    except Exception as e:
        st.error("⚠️ Database Not Set Up or Error Connecting")
        st.info(
            f"""
        **The database tables might not be created yet, or Supabase credentials are missing.**

        To set up your database:

        1. Go to your Supabase SQL Editor
        2. Create the materialized view with this SQL:
```sql
        DROP MATERIALIZED VIEW IF EXISTS public.wide_view_mv;

        CREATE MATERIALIZED VIEW public.wide_view_mv AS
        SELECT 
          DATE(reading_datetime) as "Date",
          TIME(reading_datetime) as "Time",
          MAX(CASE WHEN location_id = '15490' THEN reading_value END) as "15490",
          MAX(CASE WHEN location_id = '16034' THEN reading_value END) as "16034",
          MAX(CASE WHEN location_id = '16041' THEN reading_value END) as "16041",
          MAX(CASE WHEN location_id = '14542' THEN reading_value END) as "14542",
          MAX(CASE WHEN location_id = '15725' THEN reading_value END) as "15725",
          MAX(CASE WHEN location_id = '16032' THEN reading_value END) as "16032",
          MAX(CASE WHEN location_id = '16045' THEN reading_value END) as "16045",
          MAX(CASE WHEN location_id = '15820' THEN reading_value END) as "15820",
          MAX(CASE WHEN location_id = '15821' THEN reading_value END) as "15821",
          MAX(CASE WHEN location_id = '15999' THEN reading_value END) as "15999",
          MAX(CASE WHEN location_id = '16026' THEN reading_value END) as "16026",
          MAX(CASE WHEN location_id = '16004' THEN reading_value END) as "16004",
          MAX(CASE WHEN location_id = '16005' THEN reading_value END) as "16005"
        FROM public.meter_readings
        GROUP BY DATE(reading_datetime), TIME(reading_datetime);

        CREATE INDEX idx_wide_view_date ON public.wide_view_mv ("Date");
        
        REFRESH MATERIALIZED VIEW public.wide_view_mv;
```

        3. Set environment variable: `SUPABASE_WIDE_VIEW=wide_view_mv`

        4. Make sure `SUPABASE_URL` and `SUPABASE_ANON_KEY` are set in
           Streamlit **secrets** or environment variables.
        """
        )
        st.error(f"Technical error: {str(e)}")


if __name__ == "__main__":
    main()
