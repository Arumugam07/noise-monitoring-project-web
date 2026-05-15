"""
location_presets.py
-------------------
Per-location noise threshold presets for persisted noise detection.

Each location has a "meaningful noise band" — the dB range that represents
genuinely elevated / sustained noise for that specific site, plus the minimum
continuous duration (minutes) before it counts as an incident.

HOW TO TUNE:
  - Pull a week of raw data for each site.
  - Look at the distribution of readings.
  - Set min_db just above the site's typical daytime baseline.
  - Set max_db at the upper realistic ceiling (not 999 — keeps results clean).
  - Set duration_minutes to the shortest burst that matters for complaints/reporting.

These defaults are reasonable starting points. Adjust them once you have
reviewed real readings per location.
"""

LOCATION_PRESETS: dict[str, dict] = {
    "15490": {
        "name": "Singapore Sports School",
        "min_db": 75.0,
        "max_db": 95.0,
        "duration_minutes": 4,
        "notes": "Sports facility — expect elevated noise during events/training sessions.",
    },
    "16034": {
        "name": "BLK 120 Serangoon North Ave 1",
        "min_db": 70.0,
        "max_db": 90.0,
        "duration_minutes": 3,
        "notes": "Residential estate — lower tolerance for sustained noise.",
    },
    "16041": {
        "name": "BLK 838 Hougang Central",
        "min_db": 70.0,
        "max_db": 90.0,
        "duration_minutes": 3,
        "notes": "Residential / commercial mix.",
    },
    "14542": {
        "name": "BLK 558 Jurong West Street 42",
        "min_db": 72.0,
        "max_db": 92.0,
        "duration_minutes": 4,
        "notes": "Residential area near main roads — slightly higher baseline.",
    },
    "15725": {
        "name": "Jurong Safra, Block C",
        "min_db": 75.0,
        "max_db": 95.0,
        "duration_minutes": 3,
        "notes": "Recreation / community club — events drive noise spikes.",
    },
    "16032": {
        "name": "AMA KENG SITE",
        "min_db": 80.0,
        "max_db": 105.0,
        "duration_minutes": 5,
        "notes": "Industrial / construction site — much higher ambient baseline expected.",
    },
    "16045": {
        "name": "BLK 19 Balam Road",
        "min_db": 70.0,
        "max_db": 90.0,
        "duration_minutes": 3,
        "notes": "Residential area.",
    },
    "15820": {
        "name": "Norcom II Tower 4",
        "min_db": 68.0,
        "max_db": 88.0,
        "duration_minutes": 3,
        "notes": "Commercial building — lower ambient, smaller band.",
    },
    "15821": {
        "name": "Blk 444 Choa Chu Kang Avenue 4",
        "min_db": 70.0,
        "max_db": 90.0,
        "duration_minutes": 3,
        "notes": "Residential area.",
    },
    "15999": {
        "name": "BLK 654B Punggol Drive",
        "min_db": 70.0,
        "max_db": 90.0,
        "duration_minutes": 3,
        "notes": "Residential — newer estate, lower baseline expected.",
    },
    "16026": {
        "name": "BLK 132B Tengah Garden Avenue",
        "min_db": 68.0,
        "max_db": 88.0,
        "duration_minutes": 3,
        "notes": "New estate — quieter baseline, tighter band.",
    },
    "16004": {
        "name": "BLK 206A Punggol Place",
        "min_db": 70.0,
        "max_db": 90.0,
        "duration_minutes": 3,
        "notes": "Residential area.",
    },
    "16005": {
        "name": "Woodlands 11",
        "min_db": 72.0,
        "max_db": 92.0,
        "duration_minutes": 4,
        "notes": "Commercial / mixed development near Causeway — busier baseline.",
    },
}
