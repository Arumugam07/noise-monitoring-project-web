#!/usr/bin/env python3

import os
from datetime import date, datetime, timedelta, timezone

import requests
from zoneinfo import ZoneInfo

API_DEFAULT = "http://139.59.223.231:3000/api/meter-sound"
SGT = ZoneInfo("Asia/Singapore")

LOCATION_ID = "15490"
TEST_DAY = date(2026, 8, 1)

# Values visibly confirmed on the raw API website.
EXPECTED = {
    "00:00": 56.4,
    "06:00": 55.2,
    "08:00": 56.1,
}


def parse_as_utc(dt_string):
    """Interpret the API's Z suffix as genuine UTC."""
    parsed = datetime.fromisoformat(
        dt_string.strip().replace("Z", "+00:00")
    )

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def parse_as_sgt_wall_time(dt_string):
    """Ignore Z and interpret the timestamp components as SGT."""
    cleaned = dt_string.strip()

    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1]

    parsed = datetime.fromisoformat(cleaned)
    parsed_sgt = parsed.replace(tzinfo=SGT)

    return parsed_sgt.astimezone(timezone.utc)


def inspect_combination(
    api_base,
    query_name,
    start_string,
    end_string,
    parser_name,
    parser,
):
    url = (
        f"{api_base}/{LOCATION_ID}"
        f"?start={start_string}&end={end_string}"
    )

    response = requests.get(url, timeout=60)
    response.raise_for_status()
    raw = response.json()

    selected = {}
    rows_in_requested_sgt_day = []

    for item in raw:
        dt_string = item.get("dt")

        if not dt_string:
            continue

        timestamp_utc = parser(dt_string)
        timestamp_sgt = timestamp_utc.astimezone(SGT)

        if timestamp_sgt.date() != TEST_DAY:
            continue

        timestamp_sgt = timestamp_sgt.replace(
            second=0,
            microsecond=0,
        )

        reading = item.get("reading")

        if reading is not None:
            reading = float(reading)

        rows_in_requested_sgt_day.append(
            (timestamp_sgt, reading)
        )

        time_text = timestamp_sgt.strftime("%H:%M")

        if time_text in EXPECTED:
            selected[time_text] = reading

    print()
    print("=" * 75)
    print(f"Combination: {query_name} + {parser_name}")
    print(f"URL: {url}")
    print(f"Raw records returned: {len(raw)}")
    print(
        f"Records belonging to {TEST_DAY} SGT: "
        f"{len(rows_in_requested_sgt_day)}"
    )

    if raw:
        print(f"First raw dt: {raw[0].get('dt')}")
        print(f"Last raw dt:  {raw[-1].get('dt')}")

    all_match = True

    for time_text, expected_value in EXPECTED.items():
        actual_value = selected.get(time_text)
        matches = actual_value == expected_value
        all_match = all_match and matches

        print(
            f"{time_text}: actual={actual_value}, "
            f"expected={expected_value}, match={matches}"
        )

    print(f"SELECTED COMBINATION MATCHES: {all_match}")


def main():
    api_base = os.getenv(
        "API_BASE_URL",
        API_DEFAULT,
    ).rstrip("/")

    # Query style 1: send the Singapore calendar date directly.
    direct_start = f"{TEST_DAY.isoformat()}T00:00:00"
    direct_end = f"{TEST_DAY.isoformat()}T23:59:59"

    # Query style 2: convert the Singapore day into UTC boundaries.
    sgt_start = datetime(
        TEST_DAY.year,
        TEST_DAY.month,
        TEST_DAY.day,
        tzinfo=SGT,
    )

    sgt_end = (
        sgt_start
        + timedelta(days=1)
        - timedelta(seconds=1)
    )

    utc_start = sgt_start.astimezone(timezone.utc)
    utc_end = sgt_end.astimezone(timezone.utc)

    converted_start = utc_start.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    converted_end = utc_end.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )

    combinations = [
        (
            "Direct date query",
            direct_start,
            direct_end,
            "Parse Z as UTC",
            parse_as_utc,
        ),
        (
            "Direct date query",
            direct_start,
            direct_end,
            "Parse as SGT wall time",
            parse_as_sgt_wall_time,
        ),
        (
            "UTC-converted query",
            converted_start,
            converted_end,
            "Parse Z as UTC",
            parse_as_utc,
        ),
        (
            "UTC-converted query",
            converted_start,
            converted_end,
            "Parse as SGT wall time",
            parse_as_sgt_wall_time,
        ),
    ]

    for combination in combinations:
        inspect_combination(api_base, *combination)


if __name__ == "__main__":
    main()
