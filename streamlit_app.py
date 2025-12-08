#!/usr/bin/env python3
"""
Streamlit app: Simple login + interactive table over Supabase wide view.

Enhanced with:
- Latest readings display
- Improved visual design
- Better UX and formatting
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


def get_noise_color(value):
    """Get color for noise level."""
    if pd.isna(value):
        return "#6c757d"  # Gray
    if value < 50:
        return "#28a745"  # Green - Quiet
    elif value < 70:
        return "#ffc107"  # Yellow - Moderate
    elif value < 85:
        return "#fd7e14"  # Orange - Loud
    else:
        return "#dc3545"  # Red - Very Loud


def get_noise_category(value):
    """Categorize noise level."""
    if pd.isna(value):
        return "N/A"
    if value < 50:
        return "Quiet"
    elif value < 70:
        return "Moderate"
    elif value < 85:
        return "Loud"
    else:
        return "Very Loud"


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


def fetch_all_data(start_date=None, end_date=None, batch_size=1000) -> pd.DataFrame:
    """Fetch ALL data matching date filters (for when value filters are active)."""
    supabase = get_client()
    all_data = []
    offset = 0
    
    try:
        while True:
            query = supabase.table(DEFAULT_VIEW).select("*")
            
            if start_date:
                query = query.gte("Date", str(start_date))
            if end_date:
                query = query.lte("Date", str(end_date))
            
            query = query.order("Date", desc=True).order("Time", desc=True).range(offset, offset + batch_size - 1)
            
            resp = query.execute()
            batch = resp.data or []
            
            if not batch:
                break
                
            all_data.extend(batch)
            
            if len(batch) < batch_size:
                break
                
            offset += batch_size
        
        return pd.DataFrame(all_data)
        
    except Exception as e:
        st.error(f"Error fetching all data from {DEFAULT_VIEW}: {e}")
        return pd.DataFrame()


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
        query = query.order("Date", desc=True).order("Time", desc=True).range(offset, offset + page_size - 1)
        
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

    # --- Keep selected location columns FIRST ---
    id_cols = [c for c in df.columns if c not in ("Date", "Time")]
    keep_ids = [lid for lid in id_cols if lid in location_ids]

    if keep_ids:
        df = df[["Date", "Time"] + keep_ids]
    else:
        df = df[["Date", "Time"]]

    # Convert to numeric BEFORE filtering
    for col in keep_ids:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --- Numeric filters: Apply ONLY to selected location columns ---
    if keep_ids and (vmin is not None or vmax is not None):
        # Create a mask for rows that have at least one valid reading in the range
        # among the SELECTED locations only
        mask = pd.Series([False] * len(df), index=df.index)
        
        for col in keep_ids:
            col_mask = df[col].notna()  # Column has a value
            if vmin is not None:
                col_mask = col_mask & (df[col] >= vmin)
            if vmax is not None:
                col_mask = col_mask & (df[col] <= vmax)
            mask = mask | col_mask  # Keep row if ANY selected column matches
        
        df = df[mask]
        
        # Now filter individual cell values: set values outside range to NaN
        # This ensures the table only shows values within the range
        if not df.empty:
            for col in keep_ids:
                if vmin is not None:
                    df.loc[df[col] < vmin, col] = pd.NA
                if vmax is not None:
                    df.loc[df[col] > vmax, col] = pd.NA

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

    # Custom CSS for better styling
    st.markdown("""
        <style>
        .main-header {
            font-size: 2.5rem;
            font-weight: 700;
            color: #1f77b4;
            margin-bottom: 0.5rem;
        }
        .sub-header {
            font-size: 1.1rem;
            color: #666;
            margin-bottom: 2rem;
        }
        .metric-card {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 1.5rem;
            border-radius: 10px;
            color: white;
            text-align: center;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .latest-reading-card {
            padding: 1rem;
            border-radius: 8px;
            margin-bottom: 1rem;
            border-left: 5px solid;
            background-color: #f8f9fa;
            transition: transform 0.2s;
        }
        .latest-reading-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 8px rgba(0,0,0,0.15);
        }
        .section-divider {
            margin: 2rem 0;
            border-top: 2px solid #e9ecef;
        }
        .info-badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 12px;
            font-size: 0.85rem;
            font-weight: 600;
            margin-left: 0.5rem;
        }
        </style>
    """, unsafe_allow_html=True)

    # Login gate
    if not login_gate():
        st.markdown('<div class="main-header">🔊 Noise Monitoring System</div>', unsafe_allow_html=True)
        st.markdown('<div class="sub-header">Real-time noise level monitoring across multiple locations in Singapore</div>', unsafe_allow_html=True)
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
        
        **Noise Level Guide:**
        - 🟢 **Quiet** (< 50 dB): Library, whisper
        - 🟡 **Moderate** (50-70 dB): Normal conversation
        - 🟠 **Loud** (70-85 dB): Traffic, alarm clock
        - 🔴 **Very Loud** (> 85 dB): Heavy traffic, machinery
        
        **Features:**
        - View latest readings from all locations
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
        value=(default_start, today),
        help="Select the date range for data display"
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
    
    # Add info message about searching all data
    value_filter_active = False
    
    use_min = st.sidebar.checkbox("Filter by minimum value", value=False)
    vmin = None
    if use_min:
        vmin = st.sidebar.number_input(
            "Minimum Value (dB)",
            value=40.0,
            help="Filter readings above this value",
        )
        value_filter_active = True

    use_max = st.sidebar.checkbox("Filter by maximum value", value=False)
    vmax = None
    if use_max:
        vmax = st.sidebar.number_input(
            "Maximum Value (dB)",
            value=100.0,
            help="Filter readings below this value",
        )
        value_filter_active = True
    
    if value_filter_active:
        st.sidebar.info("🔍 Value filters will search ALL data in date range (may take longer)")

    st.sidebar.markdown("---")

    st.sidebar.subheader("📄 Pagination")
    
    if not value_filter_active:
        page = st.sidebar.number_input(
            "Page Number",
            min_value=0,
            value=0,
            step=1,
            help=f"Navigate through pages (each page shows {PAGE_SIZE} rows)",
        )
    else:
        page = 0
        st.sidebar.info("Pagination disabled when value filters are active")

    if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
        st.rerun()

    # Main content
    st.markdown('<div class="main-header">🔊 Noise Monitoring System</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Real-time noise level monitoring across multiple locations in Singapore</div>', unsafe_allow_html=True)

    try:
        # Determine if we need to fetch all data or just one page
        value_filter_active = (vmin is not None) or (vmax is not None)
        
        with st.spinner("Loading data from database..."):
            if value_filter_active:
                # Fetch ALL data when value filters are active
                st.info(f"🔍 Searching all records for values matching your criteria... This may take a moment.")
                df = fetch_all_data(start_date, end_date)
                filtered = filter_frame(df, start_date, end_date, selected_ids, vmin, vmax)
                
                # Show how many results found
                if not filtered.empty:
                    st.success(f"✅ Found {len(filtered)} records matching your filter criteria")
            else:
                # Normal pagination when no value filters
                df = fetch_page(page, PAGE_SIZE, start_date, end_date)
                filtered = filter_frame(df, start_date, end_date, selected_ids, vmin, vmax)

        if not filtered.empty:
            # === LATEST READINGS SECTION ===
            st.markdown("### 🔴 Latest Readings")
            st.caption("Most recent noise levels from selected monitoring stations")
            
            if not filtered.empty:
                latest_row = filtered.iloc[0]  # First row is most recent due to desc order
                
                # Display latest reading time
                if "Date" in latest_row.index and "Time" in latest_row.index:
                    latest_time = f"{latest_row['Date']} {latest_row['Time']}"
                    st.info(f"📅 Last updated: **{latest_time}**")
                
                # Check if any value filters are active
                value_filter_active = (vmin is not None) or (vmax is not None)
                
                # Create cards for each location
                location_cols = [c for c in filtered.columns if c not in ("Date", "Time")]
                
                # Track offline stations (only when NO value filters are active)
                offline_stations = []
                if not value_filter_active:
                    for loc in location_cols:
                        if loc in latest_row.index and pd.isna(latest_row[loc]):
                            offline_stations.append(loc)
                
                # Show offline alert if any stations are down
                if offline_stations and not value_filter_active:
                    st.error(f"⚠️ **{len(offline_stations)} Station(s) Offline** - No recent data received")
                
                # Display in rows of 3 cards
                for i in range(0, len(location_cols), 3):
                    cols = st.columns(3)
                    for j, col_obj in enumerate(cols):
                        if i + j < len(location_cols):
                            loc = location_cols[i + j]
                            if loc in latest_row.index and not pd.isna(latest_row[loc]):
                                value = latest_row[loc]
                                color = get_noise_color(value)
                                category = get_noise_category(value)
                                
                                with col_obj:
                                    st.markdown(
                                        f"""
                                        <div class="latest-reading-card" style="border-left-color: {color};">
                                            <div style="font-size: 0.9rem; font-weight: 600; color: #333; margin-bottom: 0.5rem;">
                                                📍 {loc}
                                            </div>
                                            <div style="font-size: 2.5rem; font-weight: bold; color: {color}; margin: 0.5rem 0;">
                                                {value:.1f} <span style="font-size: 1.2rem;">dB</span>
                                            </div>
                                            <div style="display: inline-block; padding: 0.25rem 0.75rem; border-radius: 12px; 
                                                 background-color: {color}; color: white; font-size: 0.85rem; font-weight: 600;">
                                                {category}
                                            </div>
                                        </div>
                                        """,
                                        unsafe_allow_html=True
                                    )
                            else:
                                with col_obj:
                                    # Different display based on whether filters are active
                                    if value_filter_active:
                                        # With filters: just show "No Data" (filtered out)
                                        st.markdown(
                                            f"""
                                            <div class="latest-reading-card" style="border-left-color: #6c757d;">
                                                <div style="font-size: 0.9rem; font-weight: 600; color: #333; margin-bottom: 0.5rem;">
                                                    📍 {loc}
                                                </div>
                                                <div style="font-size: 1.5rem; color: #999; margin: 1rem 0;">
                                                    No Data
                                                </div>
                                                <div style="font-size: 0.85rem; color: #666;">
                                                    Outside filter range
                                                </div>
                                            </div>
                                            """,
                                            unsafe_allow_html=True
                                        )
                                    else:
                                        # No filters: show as OFFLINE with warning
                                        st.markdown(
                                            f"""
                                            <div class="latest-reading-card" style="border-left-color: #dc3545; background-color: #fff5f5;">
                                                <div style="font-size: 0.9rem; font-weight: 600; color: #333; margin-bottom: 0.5rem;">
                                                    📍 {loc}
                                                </div>
                                                <div style="font-size: 2rem; font-weight: bold; color: #dc3545; margin: 0.5rem 0;">
                                                    ⚠️ OFFLINE
                                                </div>
                                                <div style="display: inline-block; padding: 0.25rem 0.75rem; border-radius: 12px; 
                                                     background-color: #dc3545; color: white; font-size: 0.85rem; font-weight: 600;">
                                                    Station Down
                                                </div>
                                            </div>
                                            """,
                                            unsafe_allow_html=True
                                        )
            
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

            # === SUMMARY STATISTICS ===
            st.markdown("### 📊 Summary Statistics")
            st.caption("Overview of the current data selection")
            
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    label="Total Records",
                    value=f"{len(filtered):,}",
                    help="Total number of readings in current view"
                )

            # Calculate statistics for numeric columns (location columns)
            numeric_cols = [c for c in filtered.columns if c not in ("Date", "Time")]
            if numeric_cols:
                all_values = []
                for col in numeric_cols:
                    all_values.extend(filtered[col].dropna().tolist())

                if all_values:
                    avg_val = sum(all_values) / len(all_values)
                    with col2:
                        st.metric(
                            label="Average Reading",
                            value=f"{avg_val:.1f} dB",
                            help="Mean noise level across all locations and times"
                        )
                    with col3:
                        st.metric(
                            label="Min Reading",
                            value=f"{min(all_values):.1f} dB",
                            help="Lowest noise level recorded",
                            delta=None,
                            delta_color="inverse"
                        )
                    with col4:
                        st.metric(
                            label="Max Reading",
                            value=f"{max(all_values):.1f} dB",
                            help="Highest noise level recorded",
                            delta=None,
                            delta_color="normal"
                        )

            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)

            # === DATA TABLE ===
            st.markdown("### 📋 Detailed Data Table")
            
            if value_filter_active:
                st.caption(
                    f"Showing **all {len(filtered)}** records matching your filter criteria. "
                    "Sorted by most recent readings first."
                )
            else:
                st.caption(
                    f"Showing **{len(filtered)}** rows from page **{page + 1}** (page size: {PAGE_SIZE}). "
                    "Sorted by most recent readings first."
                )

            display_df = filtered.copy()
            if "Date" in display_df.columns:
                display_df["Date"] = pd.to_datetime(display_df["Date"]).dt.strftime(
                    "%Y-%m-%d"
                )
            if "Time" in display_df.columns:
                display_df["Time"] = display_df["Time"].astype(str)

            # Format numeric columns
            format_dict = {
                col: "{:.2f}" for col in numeric_cols if col in display_df.columns
            }
            
            if format_dict:
                styled_df = display_df.style.format(format_dict, na_rep="—")
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

            # === EXPORT SECTION ===
            st.markdown('<div class="section-divider"></div>', unsafe_allow_html=True)
            st.markdown("### 📥 Export Data")
            st.caption("Download the current filtered dataset in your preferred format")

            col_dl1, col_dl2 = st.columns(2)

            with col_dl1:
                csv = filtered.to_csv(index=False)
                timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                filename = f"noise_readings_{timestamp}.csv"
                st.download_button(
                    label="📄 Download as CSV",
                    data=csv,
                    file_name=filename,
                    mime="text/csv",
                    use_container_width=True,
                    help="Download the currently filtered and displayed data in CSV format",
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
                        help="Download data in Excel format (.xlsx) for advanced analysis",
                    )
                except Exception:
                    st.info("💡 Excel export temporarily unavailable. Please use CSV format.")
        else:
            st.warning("⚠️ No data found matching your filters.")
            st.info(
                """
            ### 💡 Suggestions:
            - **Expand Date Range**: Try selecting a wider date range
            - **Check Locations**: Ensure you have selected at least one location
            - **Adjust Value Filters**: Remove or modify min/max value constraints
            - **Reset Page**: Navigate back to page 0
            - **Verify Data**: Ensure data exists in the database for the selected period
            """
            )
    except Exception as e:
        st.error("⚠️ Database Connection Error")
        
        with st.expander("🔧 Setup Instructions", expanded=True):
            st.markdown(
                """
            **The database might not be configured yet, or credentials are missing.**

            ### Setup Steps:

            1. **Create the materialized view** in your Supabase SQL Editor:
            
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

            2. **Set environment variables** or Streamlit secrets:
               - `SUPABASE_URL`: Your Supabase project URL
               - `SUPABASE_ANON_KEY`: Your Supabase anonymous key
               - `SUPABASE_WIDE_VIEW=wide_view_mv`
               - `APP_USERNAME`: Login username (default: admin)
               - `APP_PASSWORD`: Login password (default: changeme)

            3. **Refresh** this page after configuration
            """
            )
        
        st.error(f"**Technical Error:** {str(e)}")


if __name__ == "__main__":
    main()
