#!/usr/bin/env python3
"""
Monthly Noise Monitoring Report Generator

Automated Features:
1. Alert if any location is offline 7+ consecutive days (below 40% completeness)
2. Generate system health summary CSV for the month
3. Detect high noise incidents (≥80 dB for 2+ minutes)

Usage: python monthly_report.py [year] [month]
Example: python monthly_report.py 2026 1  # For January 2026
"""

import os
import sys
from datetime import datetime, timedelta, date
from calendar import monthrange
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Supabase configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ERROR: SUPABASE_URL and SUPABASE_ANON_KEY must be set")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# Location mapping (same as your streamlit_app.py)
LOCATIONS = {
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

READINGS_PER_DAY = 1440  # 60 readings/hour × 24 hours
OFFLINE_THRESHOLD = 0.40  # 40% completeness


def fetch_month_data(year, month):
    """Fetch all meter readings for a specific month."""
    first_day = date(year, month, 1)
    last_day = date(year, month, monthrange(year, month)[1])
    
    print(f"📅 Fetching data: {first_day} to {last_day}")
    
    all_data = []
    offset = 0
    batch_size = 1000
    
    while True:
        try:
            resp = supabase.table("meter_readings").select("*") \
                .gte("reading_datetime", f"{first_day}T00:00:00Z") \
                .lte("reading_datetime", f"{last_day}T23:59:59Z") \
                .order("reading_datetime") \
                .range(offset, offset + batch_size - 1) \
                .execute()
            
            batch = resp.data or []
            
            if not batch:
                break
            
            all_data.extend(batch)
            print(f"   Loaded {len(all_data):,} records...", end='\r')
            
            if len(batch) < batch_size:
                break
            
            offset += batch_size
            
        except Exception as e:
            print(f"\n❌ Error fetching data: {e}")
            break
    
    print(f"\n✅ Total records loaded: {len(all_data):,}")
    return pd.DataFrame(all_data)


def detect_consecutive_offline_days(df, year, month):
    """
    Detect locations that were offline for 7+ consecutive days.
    Offline = less than 40% of expected readings per day.
    """
    first_day = date(year, month, 1)
    last_day = date(year, month, monthrange(year, month)[1])
    
    print("\n🔍 Checking for 7+ consecutive offline days...")
    
    alerts = []
    
    for loc_id, loc_name in LOCATIONS.items():
        # Filter data for this location
        loc_data = df[df['location_id'] == loc_id].copy()
        loc_data['date'] = pd.to_datetime(loc_data['reading_datetime']).dt.date
        
        # Count readings per day
        daily_counts = loc_data.groupby('date').size().to_dict()
        
        # Track consecutive offline days
        consecutive_offline = 0
        offline_start = None
        
        for single_date in pd.date_range(first_day, last_day, freq='D'):
            single_date = single_date.date()
            
            day_count = daily_counts.get(single_date, 0)
            completeness = (day_count / READINGS_PER_DAY) if READINGS_PER_DAY > 0 else 0
            
            if completeness < OFFLINE_THRESHOLD:
                # This day is offline
                if consecutive_offline == 0:
                    offline_start = single_date
                consecutive_offline += 1
            else:
                # This day is online - check if previous streak was 7+ days
                if consecutive_offline >= 7:
                    offline_end = single_date - timedelta(days=1)
                    alerts.append({
                        'location_id': loc_id,
                        'location_name': loc_name,
                        'offline_start': offline_start,
                        'offline_end': offline_end,
                        'consecutive_days': consecutive_offline,
                    })
                    print(f"   ⚠️  {loc_name}: {consecutive_offline} days offline ({offline_start} to {offline_end})")
                
                # Reset counter
                consecutive_offline = 0
                offline_start = None
        
        # Check if still offline at end of month
        if consecutive_offline >= 7:
            alerts.append({
                'location_id': loc_id,
                'location_name': loc_name,
                'offline_start': offline_start,
                'offline_end': last_day,
                'consecutive_days': consecutive_offline,
            })
            print(f"   ⚠️  {loc_name}: {consecutive_offline} days offline ({offline_start} to {last_day})")
    
    if not alerts:
        print("   ✅ No locations offline for 7+ consecutive days")
    
    return alerts


def generate_system_health_report(df, year, month):
    """Generate monthly system health summary for all locations."""
    first_day = date(year, month, 1)
    last_day = date(year, month, monthrange(year, month)[1])
    total_days = (last_day - first_day).days + 1
    
    print("\n📊 Generating system health summary...")
    
    summary = []
    critical_locations = []  # Locations below 40%
    
    for loc_id, loc_name in LOCATIONS.items():
        loc_data = df[df['location_id'] == loc_id]
        
        total_readings = len(loc_data)
        expected_readings = READINGS_PER_DAY * total_days
        completeness_pct = (total_readings / expected_readings * 100) if expected_readings > 0 else 0
        
        # Determine status
        if completeness_pct >= 70:
            status = 'ONLINE'
        elif completeness_pct >= 40:
            status = 'DEGRADED'
        else:
            status = 'OFFLINE'
            critical_locations.append({
                'name': loc_name,
                'completeness': completeness_pct
            })
        
        summary.append({
            'Location': loc_name,
            'Total_Readings': total_readings,
            'Expected_Readings': expected_readings,
            'Completeness_%': round(completeness_pct, 2),
            'Status': status
        })
    
    summary_df = pd.DataFrame(summary)
    print(summary_df.to_string(index=False))
    
    return summary_df, critical_locations


def detect_high_noise_incidents(df, min_db=80, min_duration=2):
    """
    Detect sustained high noise incidents.
    
    Parameters:
    - min_db: Minimum decibel level (default: 80)
    - min_duration: Minimum consecutive minutes (default: 2)
    """
    print(f"\n🔊 Detecting noise ≥{min_db} dB for {min_duration}+ minutes...")
    
    df_sorted = df.sort_values(['location_id', 'reading_datetime']).copy()
    incidents = []
    
    for loc_id, loc_name in LOCATIONS.items():
        loc_data = df_sorted[df_sorted['location_id'] == loc_id].copy()
        
        if loc_data.empty:
            continue
        
        current_incident = None
        incident_values = []
        
        for idx, row in loc_data.iterrows():
            value = row.get('reading_value')
            
            if pd.notna(value) and value >= min_db:
                # High noise - continue or start incident
                if current_incident is None:
                    current_incident = {
                        'location_name': loc_name,
                        'start_time': row['reading_datetime']
                    }
                    incident_values = [value]
                else:
                    incident_values.append(value)
            else:
                # Below threshold or no data - check if incident should be recorded
                if current_incident and len(incident_values) >= min_duration:
                    # Get previous row's timestamp as end time
                    prev_idx = loc_data.index.get_loc(idx) - 1
                    prev_row = loc_data.iloc[prev_idx]
                    
                    incidents.append({
                        'Location': current_incident['location_name'],
                        'Start_Time': pd.to_datetime(current_incident['start_time']).strftime('%Y-%m-%d %H:%M:%S'),
                        'End_Time': pd.to_datetime(prev_row['reading_datetime']).strftime('%Y-%m-%d %H:%M:%S'),
                        'Duration_Minutes': len(incident_values),
                        'Peak_dB': round(max(incident_values), 2),
                        'Average_dB': round(sum(incident_values) / len(incident_values), 2)
                    })
                
                # Reset
                current_incident = None
                incident_values = []
        
        # Check for ongoing incident at end
        if current_incident and len(incident_values) >= min_duration:
            last_row = loc_data.iloc[-1]
            incidents.append({
                'Location': current_incident['location_name'],
                'Start_Time': pd.to_datetime(current_incident['start_time']).strftime('%Y-%m-%d %H:%M:%S'),
                'End_Time': pd.to_datetime(last_row['reading_datetime']).strftime('%Y-%m-%d %H:%M:%S'),
                'Duration_Minutes': len(incident_values),
                'Peak_dB': round(max(incident_values), 2),
                'Average_dB': round(sum(incident_values) / len(incident_values), 2)
            })
    
    incidents_df = pd.DataFrame(incidents)
    
    if not incidents_df.empty:
        print(f"   ⚠️  Found {len(incidents_df)} incidents")
        print(f"\n{incidents_df.head(10).to_string(index=False)}")
    else:
        print("   ✅ No high noise incidents detected")
    
    return incidents_df


def save_reports(health_df, incidents_df, offline_alerts, critical_locations, year, month):
    """Save all reports as CSV files."""
    # Create reports directory
    os.makedirs("reports", exist_ok=True)
    
    month_str = f"{year}-{month:02d}"
    
    print(f"\n💾 Saving reports...")
    
    # 1. System health report
    health_file = f"reports/system_health_{month_str}.csv"
    health_df.to_csv(health_file, index=False)
    print(f"   ✅ {health_file}")
    
    # 2. High noise incidents
    if not incidents_df.empty:
        incidents_file = f"reports/high_noise_incidents_{month_str}.csv"
        incidents_df.to_csv(incidents_file, index=False)
        print(f"   ✅ {incidents_file}")
    
    # 3. Offline alerts (7+ days) - if any
    if offline_alerts:
        alerts_df = pd.DataFrame(offline_alerts)
        alerts_file = f"reports/offline_alerts_{month_str}.csv"
        alerts_df.to_csv(alerts_file, index=False)
        print(f"   ✅ {alerts_file}")
    
    # 4. Critical locations (below 40%) - if any
    if critical_locations:
        critical_df = pd.DataFrame(critical_locations)
        critical_file = f"reports/critical_locations_{month_str}.csv"
        critical_df.to_csv(critical_file, index=False)
        print(f"   ✅ {critical_file}")


def print_summary(offline_alerts, critical_locations, incidents_df):
    """Print alert summary."""
    print("\n" + "="*70)
    print("🚨 ALERT SUMMARY")
    print("="*70)
    
    has_alerts = False
    
    # 7+ day offline alerts
    if offline_alerts:
        has_alerts = True
        print(f"\n⚠️  OFFLINE ALERTS (7+ Consecutive Days): {len(offline_alerts)}")
        for alert in offline_alerts:
            print(f"   • {alert['location_name']}")
            print(f"     {alert['offline_start']} to {alert['offline_end']} ({alert['consecutive_days']} days)")
    
    # Below 40% completeness
    if critical_locations:
        has_alerts = True
        print(f"\n🔴 CRITICAL LOCATIONS (Below 40% Completeness): {len(critical_locations)}")
        for loc in critical_locations:
            print(f"   • {loc['name']}: {loc['completeness']:.2f}%")
    
    if not has_alerts:
        print("\n✅ NO ALERTS - All systems operational")
    
    print(f"\n📊 High Noise Incidents (≥80 dB, 2+ min): {len(incidents_df)}")
    print("="*70)


def main():
    """Main function to generate monthly report."""
    # Determine which month to process
    if len(sys.argv) >= 3:
        year = int(sys.argv[1])
        month = int(sys.argv[2])
    else:
        # Default: last month
        today = datetime.now()
        last_month = today.replace(day=1) - timedelta(days=1)
        year = last_month.year
        month = last_month.month
    
    print(f"\n{'='*70}")
    print(f"🔊 MONTHLY NOISE MONITORING REPORT - {year}-{month:02d}")
    print(f"{'='*70}")
    
    # 1. Fetch data
    df = fetch_month_data(year, month)
    
    if df.empty:
        print("\n❌ No data found for this month")
        return
    
    # 2. Detect 7+ day offline periods
    offline_alerts = detect_consecutive_offline_days(df, year, month)
    
    # 3. Generate system health summary
    health_df, critical_locations = generate_system_health_report(df, year, month)
    
    # 4. Detect high noise incidents (≥80 dB for 2+ min)
    incidents_df = detect_high_noise_incidents(df, min_db=80, min_duration=2)
    
    # 5. Save all reports
    save_reports(health_df, incidents_df, offline_alerts, critical_locations, year, month)
    
    # 6. Print summary
    print_summary(offline_alerts, critical_locations, incidents_df)
    
    print(f"\n{'='*70}")
    print("✅ REPORT GENERATION COMPLETE")
    print(f"{'='*70}\n")
    
    print("📋 Files Generated:")
    print(f"   - reports/system_health_{year}-{month:02d}.csv")
    if not incidents_df.empty:
        print(f"   - reports/high_noise_incidents_{year}-{month:02d}.csv")
    if offline_alerts:
        print(f"   - reports/offline_alerts_{year}-{month:02d}.csv")
    if critical_locations:
        print(f"   - reports/critical_locations_{year}-{month:02d}.csv")
    print()


if __name__ == "__main__":
    main()
