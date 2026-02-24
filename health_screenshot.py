#!/usr/bin/env python3
"""
Screenshot Streamlit app after login - captures sensor health page
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

        # Step 1: Open the app
        page.goto(STREAMLIT_URL, timeout=60000)

        # Step 2: Wait until username field actually appears
        page.get_by_placeholder("Enter your username").wait_for(timeout=60000)

        # Step 3: Fill in login form
        page.get_by_placeholder("Enter your username").fill(username)
        page.get_by_placeholder("Enter your password").fill(password)

        # Step 4: Click Sign In
        page.get_by_text("Sign In").click()

        # Step 5: Wait for dashboard to actually render
        page.wait_for_selector("text=Sensor Status", timeout=60000)
        page.wait_for_timeout(5000)

        # Step 6: Screenshot
        page.screenshot(path=output_path, full_page=True)

        browser.close()

    return output_path
