#!/usr/bin/env python3
"""
Screenshot Streamlit app after login
"""
import os
from playwright.sync_api import sync_playwright

STREAMLIT_URL = "https://noise-monitoring-project-web.streamlit.app"

def screenshot_streamlit_health(output_path="health_alert.png"):
    
    username = os.getenv("APP_USERNAME", "afic")
    password = os.getenv("APP_PASSWORD", "Password3!")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"]
        )
        page = browser.new_page(
            viewport={"width": 1600, "height": 1200}
        )

        # Step 1: Open the app and wait for network to settle
        page.goto(STREAMLIT_URL, timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)
        page.wait_for_timeout(5000)

        # Step 2: Debug screenshot
        page.screenshot(path="debug_before_login.png", full_page=True)
        print(f"DEBUG: URL: {page.url}")
        print(f"DEBUG: Title: {page.title()}")

        # Step 3: Find all inputs on the page
        inputs = page.locator("input").all()
        print(f"DEBUG: Found {len(inputs)} input(s) on page")

        # Step 4: Fill username (first input) and password (second input)
        page.locator("input").nth(0).fill(username)
        page.locator("input[type='password']").fill(password)

        # Step 5: Click Sign In
        page.locator("button", has_text="Sign In").click()

        # Step 6: Wait for dashboard
        page.wait_for_load_state("networkidle", timeout=60000)
        page.wait_for_timeout(8000)

        # Step 7: Debug after login
        page.screenshot(path="debug_after_login.png", full_page=True)
        print(f"DEBUG: After login URL: {page.url}")

        # Step 8: Final screenshot
        page.screenshot(path=output_path, full_page=True)

        browser.close()

    return output_path
