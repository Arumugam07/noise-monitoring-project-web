#!/usr/bin/env python3
"""Test script - fetch data for a specific date"""
import os
import sys
from datetime import date
from dotenv import load_dotenv
from supabase import create_client
from supabase_common import API_DEFAULT, LOCATIONS, build_rows, upsert_rows

def main():
    load_dotenv()
    
    # Get date from command line or use today
    if len(sys.argv) > 1:
        test_date = date.fromisoformat(sys.argv[1])
    else:
        test_date = date.today()
    
    print(f"Testing data fetch for: {test_date}")
    print("=" * 50)
    
    # Connect to Supabase
    supabase = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_ANON_KEY"]
    )
    
    # Fetch data for all locations
    all_rows = []
    for loc in LOCATIONS:
        print(f"Fetching {loc['Name']}...", end=" ")
        rows = build_rows(API_DEFAULT, loc, test_date)
        all_rows.extend(rows)
        print(f"✓ {len(rows)} readings")
    
    print(f"\nTotal rows fetched: {len(all_rows)}")
    
    # Insert into database
    if all_rows:
        print("Inserting into database...")
        affected = upsert_rows(supabase, "meter_readings", all_rows)
        print(f"✅ Successfully inserted/updated {affected} rows")
    else:
        print("⚠️  No data to insert")
    
    # Show summary
    print("\n" + "=" * 50)
    print("SUMMARY:")
    print(f"Date: {test_date}")
    print(f"Locations: {len(LOCATIONS)}")
    print(f"Rows: {len(all_rows)}")
    print(f"Affected: {affected if all_rows else 0}")

if __name__ == "__main__":
    main()
