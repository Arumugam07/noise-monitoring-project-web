#!/usr/bin/env python3
"""
Screenshot only the Sensor Health Summary section
"""

from playwright.sync_api import sync_playwright

STREAMLIT_URL = "https://noise-monitoring-project-web.streamlit.app"

def screenshot_streamlit_health(output_path="health_alert.png"):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1600, "height": 1200})

        page.goto(STREAMLIT_URL, wait_until="networkidle", timeout=60000)

        # Wait until health section appears
        page.wait_for_selector("text=Sensor Health Summary", timeout=60000)

        # Locate the section container
        section = page.locator("text=Sensor Health Summary").first

        # Scroll into view
        section.scroll_into_view_if_needed()
        page.wait_for_timeout(3000)

        # Screenshot only the health section area
        section.screenshot(path=output_path)

        browser.close()

    return output_path
