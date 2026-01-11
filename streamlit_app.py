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
READINGS_PER_DAY = 1440  # 60 min/hour * 24 hours
# Adjusted thresholds for real-world data with gaps
OFFLINE_THRESHOLD = 0.30  # < 30% data = offline (was 10%)
DEGRADED_THRESHOLD = 0.70  # < 70% data = degraded (was 90%)
# For overall status across date range
ONLINE_OVERALL_THRESHOLD = 0.70  # ≥70% total readings = operational
DEGRADED_OVERALL_THRESHOLD = 0.40  # 40-70% = degraded


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


def get_sensor_health_single_date(df, target_date, location_cols):
    """Calculate sensor health for a single date (per-day accuracy)."""
    day_df = df[df['Date'] == target_date]

    if day_df.empty:
        return {loc: {'reading_count': 0, 'completeness': 0.0, 'status': 'OFFLINE'}
                for loc in location_cols}

    health = {}
    for loc in location_cols:
        valid_count = day_df[loc].notna().sum() if loc in day_df.columns else 0
        completeness = (valid_count / READINGS_PER_DAY * 100) if READINGS_PER_DAY > 0 else 0

        if completeness >= DEGRADED_THRESHOLD * 100:
            status = 'ONLINE'
        elif completeness >= OFFLINE_THRESHOLD * 100:
            status = 'DEGRADED'
        else:
            status = 'OFFLINE'

        health[loc] = {
            'reading_count': valid_count,
            'completeness': completeness,
            'status': status
        }

    return health


def get_sensor_health_date_range(df, start_date, end_date, location_cols):
    """Calculate sensor health across a date range (per-day accuracy)."""
    total_days = (end_date - start_date).days + 1
    health = {}

    for loc in location_cols:
        online_days = 0
        offline_dates = []
        degraded_dates = []

        for single_date in pd.date_range(start_date, end_date, freq='D'):
            single_date = single_date.date()
            day_df = df[df['Date'] == single_date]

            if day_df.empty or loc not in day_df.columns:
                offline_dates.append(single_date)
                continue

            valid_count = day_df[loc].notna().sum()

            # A day is only offline if it has ZERO readings
            # Any readings = online (even if degraded)
            if valid_count == 0:
                offline_dates.append(single_date)
            else:
                # Has some readings = counts as online day
                online_days += 1

                # But track if it was degraded (low data quality)
                completeness = (valid_count / READINGS_PER_DAY * 100) if READINGS_PER_DAY > 0 else 0
                if completeness < OFFLINE_THRESHOLD * 100:
                    degraded_dates.append(single_date)

        total_readings = df[loc].notna().sum() if loc in df.columns else 0
        expected_readings = READINGS_PER_DAY * total_days
        uptime_pct = (online_days / total_days * 100) if total_days > 0 else 0
        completeness_pct = (total_readings / expected_readings * 100) if expected_readings > 0 else 0

        # Determine status based on OVERALL completeness, not just uptime days
        if completeness_pct >= ONLINE_OVERALL_THRESHOLD * 100:
            status = 'ONLINE'
        elif completeness_pct >= DEGRADED_OVERALL_THRESHOLD * 100:
            status = 'DEGRADED'
        else:
            status = 'OFFLINE'

        health[loc] = {
            'online_days': online_days,
            'total_days': total_days,
            'uptime_pct': uptime_pct,
            'completeness_pct': completeness_pct,
            'total_readings': total_readings,
            'expected_readings': expected_readings,
            'status': status,
            'offline_dates': offline_dates,
            'degraded_dates': degraded_dates
        }

    return health


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


def show_login_page():
    """Display the enhanced login page."""
    
    # If already authenticated, don't show login
    if st.session_state.get("auth", False):
        return
    
    # Custom CSS for login page
    st.markdown("""
        <style>
        .login-container {
            max-width: 400px;
            margin: 0 auto;
            padding: 2rem;
        }
        .login-header {
            text-align: center;
            margin-bottom: 2rem;
        }
        .login-icon {
            font-size: 4rem;
            margin-bottom: 1rem;
        }
        .login-title {
            font-size: 2rem;
            font-weight: 700;
            color: #1f77b4;
            margin-bottom: 0.5rem;
        }
        .login-subtitle {
            font-size: 1rem;
            color: #666;
        }
        .login-card {
            background: white;
            padding: 2rem;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            border: 1px solid #e9ecef;
        }
        .feature-list {
            background: #f8f9fa;
            padding: 1.5rem;
            border-radius: 10px;
            margin-top: 2rem;
        }
        .feature-item {
            display: flex;
            align-items: center;
            margin-bottom: 0.75rem;
            font-size: 0.95rem;
            color: #495057;
        }
        .feature-icon {
            margin-right: 0.75rem;
            font-size: 1.2rem;
        }
        .divider {
            height: 1px;
            background: linear-gradient(to right, transparent, #dee2e6, transparent);
            margin: 1.5rem 0;
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Center the login form
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        st.markdown("""
            <div class="login-header">
                <div class="login-icon">🔊</div>
                <div class="login-title">Noise Monitoring System</div>
                <div class="login-subtitle">Real-time Environmental Noise Tracking</div>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown('<div class="login-card">', unsafe_allow_html=True)
        
        st.markdown("### 🔐 Sign In")
        st.markdown("Please enter your credentials to access the system")
        
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
        
        # Login form
        with st.form("login_form"):
            username = st.text_input(
                "Username",
                placeholder="Enter your username",
                help="Default: admin"
            )
            password = st.text_input(
                "Password",
                type="password",
                placeholder="Enter your password",
                help="Default: changeme"
            )
            
            col_a, col_b, col_c = st.columns([1, 2, 1])
            with col_b:
                submit = st.form_submit_button("🔓 Sign In", use_container_width=True, type="primary")
            
            if submit:
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
                
                if username == valid_user and password == valid_pwd:
                    st.session_state["auth"] = True
                    st.success("✅ Login successful! Redirecting...")
                    st.balloons()
                    st.rerun()
                else:
                    st.error("❌ Invalid username or password. Please try again.")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Features section
        st.markdown("""
            <div class="feature-list">
                <div style="font-weight: 600; margin-bottom: 1rem; color: #495057;">
                    📋 System Features:
                </div>
                <div class="feature-item">
                    <span class="feature-icon">🔴</span>
                    <span>Real-time monitoring across 13 locations</span>
                </div>
                <div class="feature-item">
                    <span class="feature-icon">📊</span>
                    <span>Advanced filtering and data analysis</span>
                </div>
                <div class="feature-item">
                    <span class="feature-icon">📥</span>
                    <span>Export data in CSV and Excel formats</span>
                </div>
                <div class="feature-item">
                    <span class="feature-icon">⚠️</span>
                    <span>Automatic offline station detection</span>
                </div>
                <div class="feature-item">
                    <span class="feature-icon">🔍</span>
                    <span>Historical data search and filtering</span>
                </div>
            </div>
        """, unsafe_allow_html=True)
        
        # Footer
        st.markdown("""
            <div style="text-align: center; margin-top: 2rem; color: #999; font-size: 0.85rem;">
                <p>🔒 Secure Access • 📍 Singapore • 🌐 Environmental Monitoring</p>
            </div>
        """, unsafe_allow_html=True)


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
        /* Hide sidebar on login page */
        [data-testid="stSidebar"] {
            display: block;
        }
        </style>
    """, unsafe_allow_html=True)

    # Check authentication
    if not st.session_state.get("auth", False):
        show_login_page()
        st.stop()
    
    # Logout button in sidebar after login
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 👤 Account")
    if st.sidebar.button("🚪 Logout", use_container_width=True, type="secondary"):
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

        with st.spinner("Loading all data from database for accurate health monitoring..."):
            # ALWAYS fetch ALL data for accurate health monitoring
            df_all = fetch_all_data(start_date, end_date)
            filtered_all = filter_frame(df_all, start_date, end_date, selected_ids, vmin, vmax)

            if value_filter_active:
                # When value filters active, use ALL data for everything
                st.info(f"🔍 Searching all records for values matching your criteria... This may take a moment.")
                filtered = filtered_all

                # Show how many results found
                if not filtered.empty:
                    st.success(f"✅ Found {len(filtered)} records matching your filter criteria")
            else:
                # Fetch paginated data for table display only
                df_page = fetch_page(page, PAGE_SIZE, start_date, end_date)
                filtered_page = filter_frame(df_page, start_date, end_date, selected_ids, vmin, vmax)

                # Use ALL data for health, paginated data for table
                filtered = filtered_all  # For health calculations
                filtered_table = filtered_page  # For table display

        if not filtered.empty:
            # Check if any value filters are active
            value_filter_active = (vmin is not None) or (vmax is not None)
            
            if value_filter_active:
                # === FILTER RESULTS SECTION ===
                st.markdown("### 🔍 Filter Results")
                st.caption("Number of readings found for each location within your filter criteria")
                
                # Display filter info
                filter_info = []
                if vmin is not None:
                    filter_info.append(f"Min: {vmin} dB")
                if vmax is not None:
                    filter_info.append(f"Max: {vmax} dB")
                st.info(f"📊 Filter Range: **{' | '.join(filter_info)}**")
                
                # Create cards for each location
                location_cols = [c for c in filtered.columns if c not in ("Date", "Time")]
                
                # Calculate counts for each location
                location_counts = {}
                for loc in location_cols:
                    if loc in filtered.columns:
                        count = filtered[loc].notna().sum()
                        location_counts[loc] = count
                
                # Sort locations by count (highest first)
                sorted_locations = sorted(location_counts.items(), key=lambda x: x[1], reverse=True)
                
                # Display in rows of 3 cards
                for i in range(0, len(sorted_locations), 3):
                    cols = st.columns(3)
                    for j, col_obj in enumerate(cols):
                        if i + j < len(sorted_locations):
                            loc, count = sorted_locations[i + j]
                            
                            # Color based on count (gradient from green to red)
                            if count == 0:
                                color = "#6c757d"  # Gray
                                intensity = "None"
                            elif count < 10:
                                color = "#28a745"  # Green
                                intensity = "Low"
                            elif count < 50:
                                color = "#ffc107"  # Yellow
                                intensity = "Medium"
                            elif count < 100:
                                color = "#fd7e14"  # Orange
                                intensity = "High"
                            else:
                                color = "#dc3545"  # Red
                                intensity = "Very High"
                            
                            with col_obj:
                                st.markdown(
                                    f"""
                                    <div class="latest-reading-card" style="border-left-color: {color};">
                                        <div style="font-size: 0.9rem; font-weight: 600; color: #333; margin-bottom: 0.5rem;">
                                            📍 {loc}
                                        </div>
                                        <div style="font-size: 2.5rem; font-weight: bold; color: {color}; margin: 0.5rem 0;">
                                            {count} <span style="font-size: 1.2rem;">times</span>
                                        </div>
                                        <div style="display: inline-block; padding: 0.25rem 0.75rem; border-radius: 12px; 
                                             background-color: {color}; color: white; font-size: 0.85rem; font-weight: 600;">
                                            {intensity} Frequency
                                        </div>
                                    </div>
                                    """,
                                    unsafe_allow_html=True
                                )
                
            else:
                # === SENSOR HEALTH MONITORING SECTION (No Filters) ===
                location_cols = [c for c in filtered.columns if c not in ("Date", "Time")]
                is_single_date = (start_date == end_date)

                if is_single_date:
                    # Single Date View
                    st.markdown(f"### 📅 Sensor Status for {start_date.strftime('%B %d, %Y')}")
                    st.caption(f"Total readings expected: {READINGS_PER_DAY:,} per sensor (one reading per minute)")

                    # Calculate health
                    health = get_sensor_health_single_date(filtered, start_date, location_cols)

                    # System summary
                    online_count = sum(1 for h in health.values() if h['status'] == 'ONLINE')
                    degraded_count = sum(1 for h in health.values() if h['status'] == 'DEGRADED')
                    offline_count = sum(1 for h in health.values() if h['status'] == 'OFFLINE')
                    system_health = (online_count / len(health) * 100) if health else 0

                    st.info(
                        f"**System Health: {system_health:.0f}%** | "
                        f"✅ {online_count} Online | "
                        f"⚠️ {degraded_count} Degraded | "
                        f"❌ {offline_count} Offline"
                    )

                    # Sort by status (offline first)
                    status_order = {'OFFLINE': 0, 'DEGRADED': 1, 'ONLINE': 2}
                    sorted_sensors = sorted(health.items(), key=lambda x: (status_order[x[1]['status']], x[0]))

                    # Render cards in rows of 3
                    for i in range(0, len(sorted_sensors), 3):
                        cols = st.columns(3)
                        for j in range(3):
                            if i + j < len(sorted_sensors):
                                loc, h = sorted_sensors[i + j]
                                colors = {
                                    'ONLINE': {'bg': '#d4edda', 'border': '#28a745', 'text': '#155724'},
                                    'DEGRADED': {'bg': '#fff3cd', 'border': '#ffc107', 'text': '#856404'},
                                    'OFFLINE': {'bg': '#f8d7da', 'border': '#dc3545', 'text': '#721c24'}
                                }
                                color = colors[h['status']]
                                icons = {'ONLINE': '✅', 'DEGRADED': '⚠️', 'OFFLINE': '❌'}
                                messages = {'ONLINE': 'Fully operational', 'DEGRADED': 'Monitor closely', 'OFFLINE': 'Needs maintenance'}

                                with cols[j]:
                                    st.markdown(
                                        f"""
                                        <div style="background-color: {color['bg']}; border-left: 5px solid {color['border']};
                                             border-radius: 8px; padding: 1rem; margin-bottom: 0.5rem; height: 160px;
                                             display: flex; flex-direction: column; justify-content: space-between;">
                                            <div style="font-size: 0.85rem; font-weight: 600; color: #333;">📍 {loc}</div>
                                            <div style="font-size: 1.5rem; font-weight: bold; color: {color['text']};">
                                                {icons[h['status']]} {h['status']}
                                            </div>
                                            <div style="font-size: 1.1rem; font-weight: 600; color: #333;">
                                                {h['reading_count']:,}/{READINGS_PER_DAY:,}
                                            </div>
                                            <div style="font-size: 0.9rem; color: #666;">{h['completeness']:.1f}% complete</div>
                                            <div style="font-size: 0.8rem; color: {color['text']};">{messages[h['status']]}</div>
                                        </div>
                                        """,
                                        unsafe_allow_html=True
                                    )

                else:
                    # Date Range View
                    total_days = (end_date - start_date).days + 1
                    expected_timestamps = READINGS_PER_DAY * total_days  # Expected rows
                    actual_timestamps = len(filtered)  # Actual rows

                    # Calculate total actual readings across all sensors
                    total_actual_readings = 0
                    total_expected_readings = expected_timestamps * len(location_cols)
                    for col in location_cols:
                        if col in filtered.columns:
                            total_actual_readings += filtered[col].notna().sum()

                    st.markdown(f"### 🔴 Sensor Health Summary ({start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')})")
                    st.caption(
                        f"Analysis period: **{total_days} days** | "
                        f"Timestamps: **{actual_timestamps:,}** of **{expected_timestamps:,}** ({actual_timestamps/expected_timestamps*100:.1f}%) | "
                        f"Total readings: **{total_actual_readings:,}** of **{total_expected_readings:,}** ({total_actual_readings/total_expected_readings*100:.1f}%)"
                    )
                    st.caption(
                        "📊 Status based on data completeness: "
                        "✅ Online (≥70%) | ⚠️ Degraded (40-70%) | ❌ Offline (<40%)"
                    )

                    # Calculate health
                    health = get_sensor_health_date_range(filtered, start_date, end_date, location_cols)

                    # System summary
                    online_count = sum(1 for h in health.values() if h['status'] == 'ONLINE')
                    degraded_count = sum(1 for h in health.values() if h['status'] == 'DEGRADED')
                    offline_count = sum(1 for h in health.values() if h['status'] == 'OFFLINE')
                    system_health = (online_count / len(health) * 100) if health else 0

                    st.info(
                        f"**Overall System Health: {system_health:.0f}%** | "
                        f"✅ {online_count} Operational | "
                        f"⚠️ {degraded_count} Degraded | "
                        f"❌ {offline_count} Critical"
                    )

                    # Sort by status (critical first)
                    status_order = {'OFFLINE': 0, 'DEGRADED': 1, 'ONLINE': 2}
                    sorted_sensors = sorted(health.items(), key=lambda x: (status_order[x[1]['status']], -len(x[1]['offline_dates'])))

                    # Render cards in rows of 3
                    for i in range(0, len(sorted_sensors), 3):
                        cols = st.columns(3)
                        for j in range(3):
                            if i + j < len(sorted_sensors):
                                loc, h = sorted_sensors[i + j]
                                colors = {
                                    'ONLINE': {'bg': '#d4edda', 'border': '#28a745', 'text': '#155724'},
                                    'DEGRADED': {'bg': '#fff3cd', 'border': '#ffc107', 'text': '#856404'},
                                    'OFFLINE': {'bg': '#f8d7da', 'border': '#dc3545', 'text': '#721c24'}
                                }
                                color = colors[h['status']]
                                # Extract color values to avoid nested dict access in f-strings
                                bg_color = color['bg']
                                border_color = color['border']
                                text_color = color['text']

                                icons = {'ONLINE': '✅', 'DEGRADED': '⚠️', 'OFFLINE': '❌'}
                                severities = {'ONLINE': 'Operational', 'DEGRADED': 'Monitor', 'OFFLINE': 'CRITICAL'}
                                icon = icons[h['status']]
                                severity = severities[h['status']]

                                # Format issue dates
                                issues_text = ""
                                if h['offline_dates']:
                                    dates_str = ', '.join([d.strftime('%b %d') for d in h['offline_dates']])
                                    issues_text = "Offline: " + dates_str
                                elif h['degraded_dates']:
                                    dates_str = ', '.join([d.strftime('%b %d') for d in h['degraded_dates']])
                                    issues_text = "Degraded: " + dates_str

                                # Build the card HTML using string formatting to avoid f-string nesting issues
                                card_html = """
                                <div style="background-color: {bg}; border-left: 5px solid {border};
                                     border-radius: 8px; padding: 1rem; margin-bottom: 0.5rem; height: 220px;
                                     display: flex; flex-direction: column; justify-content: space-between;">
                                    <div style="font-size: 0.85rem; font-weight: 600; color: #333;">📍 {location}</div>
                                    <div style="font-size: 1.3rem; font-weight: bold; color: {txt_color};">
                                        {icon} {status} ({pct:.0f}%)
                                    </div>
                                    <div style="font-size: 0.9rem; color: #333;">
                                        <strong>Days online:</strong> {online}/{total}
                                    </div>
                                    <div style="font-size: 0.85rem; color: #666;">
                                        <strong>Readings:</strong> {readings:,}/{expected:,}
                                    </div>
                                    {issues}
                                    <div style="font-size: 0.8rem; font-weight: 600; color: {txt_color}; margin-top: 0.25rem;">
                                        {sev}
                                    </div>
                                </div>
                                """.format(
                                    bg=bg_color,
                                    border=border_color,
                                    location=loc,
                                    txt_color=text_color,
                                    icon=icon,
                                    status=h['status'],
                                    pct=h['completeness_pct'],
                                    online=h['online_days'],
                                    total=h['total_days'],
                                    readings=h['total_readings'],
                                    expected=h['expected_readings'],
                                    issues=('<div style="font-size: 0.75rem; color: ' + text_color + '; margin-top: 0.25rem;">' + issues_text + '</div>') if issues_text else '',
                                    sev=severity
                                )

                                with cols[j]:
                                    st.markdown(
                                        card_html,
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
                    help="Total number of readings in selected date range (all data, not just current page)"
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

            # Use appropriate dataset for table display
            if value_filter_active:
                table_data = filtered  # All data when filtering
                st.caption(
                    f"Showing **all {len(table_data)}** records matching your filter criteria. "
                    "Sorted by most recent readings first."
                )
            else:
                table_data = filtered_table  # Paginated data when no filters
                st.caption(
                    f"Showing **{len(table_data)}** rows from page **{page + 1}** (page size: {PAGE_SIZE}). "
                    "Sorted by most recent readings first."
                )

            display_df = table_data.copy()
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
