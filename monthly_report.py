#!/usr/bin/env python3
"""
Monthly Noise Monitoring Report Generator with HTML Visual Reports

Generates:
1. CSV reports (system health, incidents, alerts)
2. HTML visual report matching the Streamlit app design
3. Only flags issues: 7+ consecutive offline days OR <40% health
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

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ ERROR: SUPABASE_URL and SUPABASE_ANON_KEY must be set")
    sys.exit(1)

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

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

READINGS_PER_DAY = 1440
OFFLINE_THRESHOLD = 0.40


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
    """Detect locations offline for 7+ consecutive days."""
    first_day = date(year, month, 1)
    last_day = date(year, month, monthrange(year, month)[1])
    
    print("\n🔍 Checking for 7+ consecutive offline days...")
    
    alerts = []
    
    for loc_id, loc_name in LOCATIONS.items():
        loc_data = df[df['location_id'] == loc_id].copy()
        loc_data['date'] = pd.to_datetime(loc_data['reading_datetime']).dt.date
        
        daily_counts = loc_data.groupby('date').size().to_dict()
        
        consecutive_offline = 0
        offline_start = None
        
        for single_date in pd.date_range(first_day, last_day, freq='D'):
            single_date = single_date.date()
            
            day_count = daily_counts.get(single_date, 0)
            completeness = (day_count / READINGS_PER_DAY) if READINGS_PER_DAY > 0 else 0
            
            if completeness < OFFLINE_THRESHOLD:
                if consecutive_offline == 0:
                    offline_start = single_date
                consecutive_offline += 1
            else:
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
                
                consecutive_offline = 0
                offline_start = None
        
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
    """Generate system health summary for LAST 7 DAYS of the month."""
    last_day = date(year, month, monthrange(year, month)[1])
    first_day_of_period = last_day - timedelta(days=6)  # Last 7 days
    analysis_days = 7
    
    print(f"\n📊 Generating system health summary (Last 7 days: {first_day_of_period} to {last_day})...")
    
    summary = []
    critical_locations = []
    
    # Filter data to last 7 days only
    df['date'] = pd.to_datetime(df['reading_datetime']).dt.date
    df_period = df[(df['date'] >= first_day_of_period) & (df['date'] <= last_day)]
    
    for loc_id, loc_name in LOCATIONS.items():
        loc_data = df_period[df_period['location_id'] == loc_id]
        
        # CRITICAL FIX: Only count rows with actual readings (not null/None)
        # Filter out null readings to match Streamlit app behavior
        loc_data_valid = loc_data[loc_data['reading_value'].notna()]
        
        # Calculate days online using only valid readings
        if not loc_data_valid.empty:
            days_with_data = pd.to_datetime(loc_data_valid['reading_datetime']).dt.date.nunique()
        else:
            days_with_data = 0
        
        # Count only actual valid readings (excludes None/null values)
        total_readings = len(loc_data_valid)
        
        # Expected readings = theoretical maximum (1440 per day × 7 days)
        expected_readings = READINGS_PER_DAY * analysis_days
        
        # Calculate completeness percentage
        completeness_pct = (total_readings / expected_readings * 100) if expected_readings > 0 else 0
        
        # Determine status based on completeness
        if completeness_pct >= 70:
            status = 'ONLINE'
        elif completeness_pct >= 40:
            status = 'DEGRADED'
        else:
            status = 'OFFLINE'
            critical_locations.append({
                'name': loc_name,
                'completeness': completeness_pct,
                'days_online': days_with_data,
                'total_days': analysis_days,
                'period_start': first_day_of_period,
                'period_end': last_day
            })
        
        summary.append({
            'Location': loc_name,
            'Days_Online': days_with_data,
            'Total_Days': analysis_days,
            'Total_Readings': total_readings,
            'Expected_Readings': expected_readings,
            'Completeness_%': round(completeness_pct, 2),
            'Status': status
        })
    
    summary_df = pd.DataFrame(summary)
    print(summary_df.to_string(index=False))
    
    return summary_df, critical_locations, first_day_of_period, last_day


def detect_high_noise_incidents(df, min_db=80, min_duration=2):
    """Detect sustained high noise incidents."""
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
                if current_incident is None:
                    current_incident = {
                        'location_name': loc_name,
                        'start_time': row['reading_datetime']
                    }
                    incident_values = [value]
                else:
                    incident_values.append(value)
            else:
                if current_incident and len(incident_values) >= min_duration:
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
                
                current_incident = None
                incident_values = []
        
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
    else:
        print("   ✅ No high noise incidents detected")
    
    return incidents_df


def generate_html_report(health_df, offline_alerts, critical_locations, incidents_df, year, month, health_period_start, health_period_end):
    """Generate visual HTML report matching Streamlit app design."""
    month_str = f"{year}-{month:02d}"
    first_day = date(year, month, 1)
    last_day = date(year, month, monthrange(year, month)[1])
    total_days_month = (last_day - first_day).days + 1
    health_days = (health_period_end - health_period_start).days + 1
    
    # Count statuses
    online_count = len(health_df[health_df['Status'] == 'ONLINE'])
    degraded_count = len(health_df[health_df['Status'] == 'DEGRADED'])
    offline_count = len(health_df[health_df['Status'] == 'OFFLINE'])
    
    total_readings = health_df['Total_Readings'].sum()
    expected_readings = health_df['Expected_Readings'].sum()
    system_health_pct = (online_count / len(health_df) * 100) if len(health_df) > 0 else 0
    
    html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Noise Monitoring Report - {month_str}</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
        }}
        .header h1 {{
            margin: 0 0 10px 0;
            font-size: 2rem;
        }}
        .header p {{
            margin: 5px 0;
            opacity: 0.9;
        }}
        .summary {{
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .summary h2 {{
            margin-top: 0;
            color: #333;
        }}
        .stats {{
            display: flex;
            gap: 20px;
            margin: 20px 0;
        }}
        .stat-card {{
            flex: 1;
            background: #f8f9fa;
            padding: 15px;
            border-radius: 8px;
            text-align: center;
        }}
        .stat-value {{
            font-size: 2rem;
            font-weight: bold;
            color: #667eea;
        }}
        .stat-label {{
            color: #666;
            margin-top: 5px;
        }}
        .sensor-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .sensor-card {{
            background: white;
            border-radius: 8px;
            padding: 20px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border-left: 5px solid;
        }}
        .sensor-card.online {{
            border-left-color: #28a745;
            background-color: #d4edda;
        }}
        .sensor-card.degraded {{
            border-left-color: #ffc107;
            background-color: #fff3cd;
        }}
        .sensor-card.offline {{
            border-left-color: #dc3545;
            background-color: #f8d7da;
        }}
        .sensor-name {{
            font-weight: 600;
            margin-bottom: 10px;
            color: #333;
        }}
        .sensor-status {{
            font-size: 1.5rem;
            font-weight: bold;
            margin: 10px 0;
        }}
        .sensor-status.online {{
            color: #155724;
        }}
        .sensor-status.degraded {{
            color: #856404;
        }}
        .sensor-status.offline {{
            color: #721c24;
        }}
        .sensor-details {{
            font-size: 0.9rem;
            color: #666;
            margin: 5px 0;
        }}
        .alert-section {{
            background: #fff3cd;
            border-left: 5px solid #ffc107;
            padding: 20px;
            border-radius: 8px;
            margin: 20px 0;
        }}
        .alert-section h3 {{
            margin-top: 0;
            color: #856404;
        }}
        .alert-item {{
            background: white;
            padding: 15px;
            margin: 10px 0;
            border-radius: 5px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            background: white;
            margin: 20px 0;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background: #667eea;
            color: white;
            font-weight: 600;
        }}
        tr:hover {{
            background: #f5f5f5;
        }}
        .footer {{
            text-align: center;
            color: #666;
            margin-top: 40px;
            padding: 20px;
        }}
        .period-note {{
            background: #e7f3ff;
            border-left: 4px solid #2196F3;
            padding: 10px 15px;
            margin: 15px 0;
            border-radius: 4px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🔊 Noise Monitoring System - Monthly Report</h1>
        <p><strong>Full Period:</strong> {first_day.strftime('%B %d, %Y')} - {last_day.strftime('%B %d, %Y')} ({total_days_month} days)</p>
        <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S SGT')}</p>
    </div>
    
    <div class="summary">
        <h2>🔴 Sensor Health Summary (Last {health_days} Days)</h2>
        <div class="period-note">
            <strong>📅 Analysis Period:</strong> {health_period_start.strftime('%B %d, %Y')} - {health_period_end.strftime('%B %d, %Y')}
        </div>
        <h3>📊 Overall System Health: {system_health_pct:.0f}%</h3>
        <div class="stats">
            <div class="stat-card">
                <div class="stat-value" style="color: #28a745;">✅ {online_count}</div>
                <div class="stat-label">Operational</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: #ffc107;">⚠️ {degraded_count}</div>
                <div class="stat-label">Degraded</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color: #dc3545;">❌ {offline_count}</div>
                <div class="stat-label">Critical</div>
            </div>
        </div>
        <p><strong>Total Readings (Last {health_days} days):</strong> {total_readings:,} of {expected_readings:,} ({total_readings/expected_readings*100:.1f}%)</p>
        <p><strong>Status Legend:</strong> ✅ Online (≥70%) | ⚠️ Degraded (40-70%) | ❌ Offline (<40%)</p>
    </div>
"""
    
    # Alerts section
    if offline_alerts or critical_locations:
        html += """
    <div class="alert-section">
        <h3>🚨 CRITICAL ALERTS DETECTED</h3>
"""
        if offline_alerts:
            html += f"<h4>⚠️ Locations Offline 7+ Consecutive Days: {len(offline_alerts)}</h4>"
            for alert in offline_alerts:
                html += f"""
        <div class="alert-item">
            <strong>📍 {alert['location_name']}</strong><br>
            Offline Period: {alert['offline_start']} to {alert['offline_end']}<br>
            Duration: {alert['consecutive_days']} consecutive days
        </div>
"""
        
        if critical_locations:
            html += f"<h4>🔴 Locations Below 40% Health: {len(critical_locations)}</h4>"
            for loc in critical_locations:
                html += f"""
        <div class="alert-item">
            <strong>📍 {loc['name']}</strong><br>
            Completeness: {loc['completeness']:.2f}%<br>
            Days Online: {loc['days_online']}/{loc['total_days']} days
        </div>
"""
        html += "    </div>\n"
    
    # Sensor health cards - ONLY show if there are critical locations (below 40%)
    if critical_locations:
        html += """
    <div class="summary">
        <h2>🔴 Critical Sensor Health Details (Below 40%)</h2>
        <div class="sensor-grid">
"""
        
        # Only show locations that are actually critical (below 40%)
        for loc in critical_locations:
            # Find the full details from health_df
            loc_row = health_df[health_df['Location'] == loc['name']].iloc[0]
            
            html += f"""
            <div class="sensor-card offline">
                <div class="sensor-name">📍 {loc['name']}</div>
                <div class="sensor-status offline">❌ OFFLINE ({loc['completeness']:.0f}%)</div>
                <div class="sensor-details">Days online: {loc['days_online']}/{loc['total_days']}</div>
                <div class="sensor-details">Readings: {loc_row['Total_Readings']:,}/{loc_row['Expected_Readings']:,}</div>
                <div class="sensor-details" style="color: #721c24; font-weight: 600; margin-top: 10px;">⚠️ REQUIRES MAINTENANCE</div>
            </div>
"""
        
        html += """
        </div>
    </div>
"""
    else:
        # All systems operational - show simple message
        html += """
    <div class="summary" style="background: #d4edda; border-left: 5px solid #28a745;">
        <h2 style="color: #155724;">✅ All Sensors Operational</h2>
        <p style="color: #155724; font-size: 1.1rem;">All 13 monitoring locations are functioning normally with completeness above 40%. No maintenance required.</p>
    </div>
"""
    
    # High noise incidents
    if not incidents_df.empty:
        html += f"""
    <div class="summary">
        <h2>🔊 High Noise Incidents (≥80 dB, 2+ minutes): {len(incidents_df)}</h2>
        <table>
            <thead>
                <tr>
                    <th>Location</th>
                    <th>Start Time</th>
                    <th>Duration (min)</th>
                    <th>Peak dB</th>
                    <th>Average dB</th>
                </tr>
            </thead>
            <tbody>
"""
        for _, incident in incidents_df.head(50).iterrows():
            html += f"""
                <tr>
                    <td>{incident['Location']}</td>
                    <td>{incident['Start_Time']}</td>
                    <td>{incident['Duration_Minutes']}</td>
                    <td>{incident['Peak_dB']}</td>
                    <td>{incident['Average_dB']}</td>
                </tr>
"""
        html += """
            </tbody>
        </table>
"""
        if len(incidents_df) > 50:
            html += f"        <p><em>Showing first 50 of {len(incidents_df)} incidents. See CSV file for complete list.</em></p>\n"
        html += "    </div>\n"
    
    html += """
    <div class="footer">
        <p>RSAF Noise Monitoring System | Automated Monthly Report</p>
        <p>For technical support, please contact the system administrator</p>
    </div>
</body>
</html>
"""
    
    return html


def save_reports(health_df, incidents_df, offline_alerts, critical_locations, year, month, health_period_start, health_period_end):
    """Save all reports."""
    os.makedirs("reports", exist_ok=True)
    
    month_str = f"{year}-{month:02d}"
    
    print(f"\n💾 Saving reports...")
    
    # 1. System health CSV
    health_file = f"reports/system_health_{month_str}.csv"
    health_df.to_csv(health_file, index=False)
    print(f"   ✅ {health_file}")
    
    # 2. High noise incidents CSV
    if not incidents_df.empty:
        incidents_file = f"reports/high_noise_incidents_{month_str}.csv"
        incidents_df.to_csv(incidents_file, index=False)
        print(f"   ✅ {incidents_file}")
    
    # 3. Offline alerts CSV
    if offline_alerts:
        alerts_df = pd.DataFrame(offline_alerts)
        alerts_file = f"reports/offline_alerts_{month_str}.csv"
        alerts_df.to_csv(alerts_file, index=False)
        print(f"   ✅ {alerts_file}")
    
    # 4. Critical locations CSV
    if critical_locations:
        critical_df = pd.DataFrame(critical_locations)
        critical_file = f"reports/critical_locations_{month_str}.csv"
        critical_df.to_csv(critical_file, index=False)
        print(f"   ✅ {critical_file}")
    
    # 5. HTML visual report
    html_content = generate_html_report(health_df, offline_alerts, critical_locations, incidents_df, year, month, health_period_start, health_period_end)
    html_file = f"reports/monthly_report_{month_str}.html"
    with open(html_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"   ✅ {html_file} (Visual Report)")


def print_summary(offline_alerts, critical_locations, incidents_df):
    """Print alert summary."""
    print("\n" + "="*70)
    print("🚨 ALERT SUMMARY")
    print("="*70)
    
    has_alerts = False
    
    if offline_alerts:
        has_alerts = True
        print(f"\n⚠️  OFFLINE ALERTS (7+ Consecutive Days): {len(offline_alerts)}")
        for alert in offline_alerts:
            print(f"   • {alert['location_name']}")
            print(f"     {alert['offline_start']} to {alert['offline_end']} ({alert['consecutive_days']} days)")
    
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
    """Main function."""
    if len(sys.argv) >= 3:
        year = int(sys.argv[1])
        month = int(sys.argv[2])
    else:
        today = datetime.now()
        last_month = today.replace(day=1) - timedelta(days=1)
        year = last_month.year
        month = last_month.month
    
    print(f"\n{'='*70}")
    print(f"🔊 MONTHLY NOISE MONITORING REPORT - {year}-{month:02d}")
    print(f"{'='*70}")
    
    df = fetch_month_data(year, month)
    
    if df.empty:
        print("\n❌ No data found for this month")
        return
    
    offline_alerts = detect_consecutive_offline_days(df, year, month)
    health_df, critical_locations, health_period_start, health_period_end = generate_system_health_report(df, year, month)
    incidents_df = detect_high_noise_incidents(df, min_db=80, min_duration=2)
    
    save_reports(health_df, incidents_df, offline_alerts, critical_locations, year, month, health_period_start, health_period_end)
    print_summary(offline_alerts, critical_locations, incidents_df)
    
    print(f"\n{'='*70}")
    print("✅ REPORT GENERATION COMPLETE")
    print(f"{'='*70}\n")
    
    print("📋 Files Generated:")
    print(f"   - reports/monthly_report_{year}-{month:02d}.html (📊 VISUAL REPORT)")
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
