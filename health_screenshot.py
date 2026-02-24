#!/usr/bin/env python3
"""
Screenshot Streamlit app after login - waits for full render
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

        # Step 1: Load the app
        page.goto(STREAMLIT_URL, timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)
        page.wait_for_timeout(5000)

        # Step 2: Find frame with inputs
        login_frame = None
        for frame in page.frames:
            inputs = frame.locator("input").all()
            if len(inputs) > 0:
                login_frame = frame
                break

        if login_frame is None:
            raise Exception("Could not find login form in any frame")

        # Step 3: Fill login form
        login_frame.locator("input").nth(0).fill(username)
        login_frame.locator("input[type='password']").fill(password)

        # Step 4: Click Sign In
        login_frame.locator("button", has_text="Sign In").click()

        # Step 5: Wait for loading spinner to DISAPPEAR
        # This means data has fully loaded
        page.wait_for_timeout(5000)
        try:
            page.wait_for_selector(
                "text=Loading all data from database",
                state="detached",
                timeout=60000
            )
            print("DEBUG: Loading spinner gone — data is ready")
        except Exception:
            print("DEBUG: Spinner wait timed out — proceeding anyway")

        # Step 6: Wait for sensor cards to appear
        try:
            page.wait_for_selector(
                "text=System Health",
                state="visible",
                timeout=60000
            )
            print("DEBUG: Sensor health cards visible")
        except Exception:
            print("DEBUG: Could not confirm sensor cards — proceeding anyway")

        # Step 7: Extra buffer for all 13 cards to render
        page.wait_for_timeout(5000)

        # Step 8: Final screenshot
        page.screenshot(path=output_path, full_page=True)
        print("DEBUG: Screenshot saved")

        browser.close()

    return output_path
