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

DEFAULT_VIEW = os.getenv("SUPABASE_WIDE_VIEW", "wide_view")
PAGE_SIZE = 200


def get_client():
    load_dotenv()
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_ANON_KEY"]
    return create_client(url, key)


def fetch_page(page: int, page_size: int) -> pd.DataFrame:
    supabase = get_client()
    offset = page * page_size
    # Try RPC for raw SQL first; fallback to simple select
    try:
        resp = supabase.postgrest.rpc(
            "exec_sql",
            {
                "sql": f"SELECT * FROM public.{DEFAULT_VIEW} ORDER BY \"Date\", \"Time\" OFFSET {offset} LIMIT {page_size}"
            },
        ).execute()
        rows = resp.data or []
        return pd.DataFrame(rows)
    except Exception:
        resp = supabase.table(DEFAULT_VIEW).select("*").execute()
        df = pd.DataFrame(resp.data or [])
        if df.empty:
            return df
        return df.sort_values(["Date", "Time"]).iloc[offset: offset + page_size]


def filter_frame(df: pd.DataFrame, date_range, location_ids, vmin, vmax) -> pd.DataFrame:
    if df.empty:
        return df

    # Date filter
    if date_range and len(date_range) == 2 and date_range[0] and date_range[1]:
        start_date, end_date = date_range
        df = df[(df["Date"] >= pd.to_datetime(start_date)) & (df["Date"] <= pd.to_datetime(end_date))]

    # Keep selected location columns
    id_cols = [c for c in df.columns if c not in ("Date", "Time")]
    keep_ids = [lid for lid in id_cols if lid in location_ids]
    df = df[["Date", "Time"] + keep_ids]

    # Numeric range across selected columns
    if keep_ids and (vmin is not None or vmax is not None):
        for col in keep_ids:
            if vmin is not None:
                df = df[(df[col].isna()) | (df[col] >= vmin)]
            if vmax is not None:
                df = df[(df[col].isna()) | (df[col] <= vmax)]

    # Rename to friendly names
    rename = {lid: LOCATION_ID_TO_NAME.get(lid, lid) for lid in keep_ids}
    return df.rename(columns=rename)


def login_gate() -> bool:
    st.sidebar.header("🔐 Authentication")
    user = st.sidebar.text_input("Username", placeholder="Enter username")
    pwd = st.sidebar.text_input("Password", type="password", placeholder="Enter password")
    if st.sidebar.button("Sign in", type="primary", use_container_width=True):
        # Get credentials from environment variables
        valid_user = os.getenv("APP_USERNAME", "admin")
        valid_pwd = os.getenv("APP_PASSWORD", "changeme")
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
        st.markdown("""
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
        """)

    st.sidebar.header("🔍 Filters")
    st.sidebar.markdown("---")

    date_range = st.sidebar.date_input(
        "📅 Date Range",
        value=None,  # allow empty
        help="Select a date range to filter readings"
    )

# Normalize date_range into start/end dates
    if isinstance(date_range, list) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = None
    
    st.sidebar.markdown("---")
    
    all_ids = list(LOCATION_ID_TO_NAME.keys())
    selected_ids = st.sidebar.multiselect(
        "📍 Locations",
        options=all_ids,
        default=all_ids,
        format_func=lambda x: LOCATION_ID_TO_NAME.get(x, x),
        help="Select one or more monitoring locations"
    )
    
    st.sidebar.markdown("---")
    
    st.sidebar.subheader("📊 Value Range (dB)")
    vmin = st.sidebar.number_input(
        "Minimum Value (dB)", 
        value=None, 
        placeholder="e.g. 40.0",
        help="Filter readings above this value"
    )
    vmax = st.sidebar.number_input(
        "Maximum Value (dB)", 
        value=None, 
        placeholder="e.g. 100.0",
        help="Filter readings below this value"
    )
    
    st.sidebar.markdown("---")
    
    st.sidebar.subheader("📄 Pagination")
    page = st.sidebar.number_input(
        "Page Number", 
        min_value=0, 
        value=0, 
        step=1,
        help=f"Navigate through pages (each page shows {PAGE_SIZE} rows)"
    )
    
    if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
        st.rerun()

    try:
        with st.spinner("Loading data from database..."):
            df = fetch_page(page, PAGE_SIZE)
            filtered = filter_frame(df, date_range, selected_ids, vmin, vmax)

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
                    with col2:
                        st.metric("Average Reading", f"{sum(all_values)/len(all_values):.2f} dB")
                    with col3:
                        st.metric("Min Reading", f"{min(all_values):.2f} dB")
                    with col4:
                        st.metric("Max Reading", f"{max(all_values):.2f} dB")
            
            st.divider()
            
            # Display the data table with enhanced formatting
            st.markdown("### 📋 Data Table")
            st.caption(f"Showing {len(filtered)} rows (page {page + 1}, page size {PAGE_SIZE}). Use filters in the sidebar to refine results.")
            
            # Format the dataframe for better display
            display_df = filtered.copy()
            if "Date" in display_df.columns:
                display_df["Date"] = pd.to_datetime(display_df["Date"]).dt.strftime("%Y-%m-%d")
            if "Time" in display_df.columns:
                display_df["Time"] = display_df["Time"].astype(str)
            
            # Format numeric columns for better display
            format_dict = {col: "{:.2f}" for col in numeric_cols if col in display_df.columns}
            if format_dict:
                styled_df = display_df.style.format(format_dict, na_rep="N/A")
                st.dataframe(
                    styled_df,
                    use_container_width=True,
                    height=600,
                    hide_index=True
                )
            else:
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    height=600,
                    hide_index=True
                )
            
            # Enhanced download functionality
            st.divider()
            st.markdown("### 📥 Export Data")
            
            col_dl1, col_dl2 = st.columns(2)
            
            with col_dl1:
                # Download current filtered view as CSV
                csv = filtered.to_csv(index=False)
                timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                filename = f"noise_readings_{timestamp}.csv"
                st.download_button(
                    label="📥 Download Current View (CSV)",
                    data=csv,
                    file_name=filename,
                    mime="text/csv",
                    use_container_width=True,
                    help="Download the currently filtered and displayed data"
                )
            
            with col_dl2:
                # Download as Excel
                try:
                    import io
                    excel_buffer = io.BytesIO()
                    filtered.to_excel(excel_buffer, index=False, engine='openpyxl')
                    excel_buffer.seek(0)
                    st.download_button(
                        label="📊 Download as Excel",
                        data=excel_buffer,
                        file_name=f"noise_readings_{timestamp}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        help="Download data in Excel format (.xlsx)"
                    )
                except Exception as e:
                    st.info("💡 Excel export temporarily unavailable")
        else:
            st.warning("⚠️ No data found matching your filters.")
            st.info("💡 Try adjusting:")
            st.markdown("""
            - **Date Range**: Select a wider date range
            - **Locations**: Select different or all locations
            - **Value Range**: Adjust or remove min/max value filters
            - **Page Number**: Try page 0 or check if data exists
            """)
    except Exception as e:
        st.error("⚠️ Database Not Set Up")
        st.info("""
        **The database tables haven't been created yet.**

        To set up your database:

        1. Go to your Supabase SQL Editor
        2. Run this SQL:

        ```sql
        CREATE TABLE IF NOT EXISTS public.meter_readings (
          location_id text NOT NULL,
          location_name text,
          reading_value double precision,
          reading_datetime timestamptz NOT NULL,
          created_at timestamptz NOT NULL DEFAULT now(),
          CONSTRAINT meter_readings_pkey PRIMARY KEY (location_id, reading_datetime)
        );

        CREATE INDEX IF NOT EXISTS idx_meter_readings_datetime
        ON public.meter_readings (reading_datetime);
        ```

        3. Then run the ETL to load data: `python supabase_daily.py`

        4. Create the wide_view (contact admin for SQL)
        """)
        st.error(f"Technical error: {str(e)}")


if __name__ == "__main__":
    main()

