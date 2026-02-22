#!/usr/bin/env python3
"""
Simple and stable full-page Streamlit screenshot
"""

from playwright.sync_api import sync_playwright

STREAMLIT_URL = "https://noise-monitoring-project-web.streamlit.app"

def screenshot_streamlit_health(output_path="health_alert.png"):

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )

        page = browser.new_page(
            viewport={"width": 1600, "height": 1200}
        )

        # Open app
        page.goto(STREAMLIT_URL, timeout=60000)

        # Give Streamlit time to fully render charts/cards
        page.wait_for_timeout(10000)

        # Take full page screenshot
        page.screenshot(path=output_path, full_page=True)

        browser.close()

    return output_path
