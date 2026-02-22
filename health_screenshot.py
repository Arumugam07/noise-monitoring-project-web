#!/usr/bin/env python3
"""
Screenshot only the Sensor Status section of Streamlit app
"""

from playwright.sync_api import sync_playwright

STREAMLIT_URL = "https://noise-monitoring-project-web.streamlit.app"

def screenshot_streamlit_health(output_path="health_alert.png"):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1600, "height": 1200})

        page.goto(STREAMLIT_URL, wait_until="networkidle", timeout=60000)

        # Wait for correct section header
        page.wait_for_selector("text=Sensor Status", timeout=60000)

        # Locate header element
        header = page.locator("text=Sensor Status").first

        # Scroll into view
        header.scroll_into_view_if_needed()

        # Give time for cards to fully render
        page.wait_for_timeout(5000)

        # Screenshot full page (safer for Streamlit layouts)
        page.screenshot(path=output_path, full_page=True)

        browser.close()

    return output_path
