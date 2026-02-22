#!/usr/bin/env python3
"""
Simple Streamlit screenshot for health check
"""

from playwright.sync_api import sync_playwright

STREAMLIT_URL = "https://noise-monitoring-project-web.streamlit.app"


def screenshot_streamlit_health(output_path="health_alert.png"):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1400, "height": 900})

        page.goto(STREAMLIT_URL, wait_until="networkidle", timeout=60000)

        # Wait for the Sensor Health header instead of login form
        page.wait_for_selector("text=Sensor Health Summary", timeout=60000)

        # Small buffer time for charts/cards
        page.wait_for_timeout(4000)

        page.screenshot(path=output_path, full_page=True)

        browser.close()

    return output_path
