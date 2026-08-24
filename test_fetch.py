import os
from datetime import date

from supabase_common import API_DEFAULT, LOCATIONS, build_rows, SGT

api_base = os.getenv("API_BASE_URL", API_DEFAULT).rstrip("/")
location = next(loc for loc in LOCATIONS if loc["ID"] == "15490")
test_day = date(2026, 8, 1)

rows = build_rows(api_base, location, test_day)

print("=" * 70)
print(f"Location: {location['Name']}")
print(f"Date: {test_day}")
print(f"Rows returned: {len(rows)}")
print("=" * 70)

# Print selected times for comparison with the raw IP website
wanted_times = {"00:00", "01:00", "06:00", "07:59", "08:00", "12:00", "22:00", "23:59"}

for row in rows:
    utc_dt = row["reading_datetime"]
    sgt_dt = __import__("datetime").datetime.fromisoformat(utc_dt).astimezone(SGT)

    if sgt_dt.strftime("%H:%M") in wanted_times:
        print(
            f"SGT {sgt_dt.strftime('%Y-%m-%d %H:%M')} | "
            f"UTC {utc_dt} | "
            f"Value {row['reading_value']}"
        )
