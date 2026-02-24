#!/usr/bin/env python3
"""
Screenshot Streamlit app after login - handles sleep/wake + login
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
        page.wait_for_timeout(5000)

        # Step 2: Handle Streamlit "wake up" screen if app is sleeping
        try:
            wake_button = page.get_by_text("Yes, get this app back up!")
            if wake_button.is_visible():
                wake_button.click()
                # Wait for app to fully wake up and load
                page.wait_for_timeout(20000)
        except Exception:
            pass  # App was already awake, continue normally

        # Step 3: Wait for login form to appear
        page.get_by_placeholder("Enter your username").wait_for(
            state="visible",
            timeout=60000
        )

        # Step 4: Fill in login form
        page.get_by_placeholder("Enter your username").fill(username)
        page.get_by_placeholder("Enter your password").fill(password)

        # Step 5: Click Sign In
        page.get_by_text("Sign In").click()

        # Step 6: Wait for dashboard to render
        page.wait_for_selector("text=Sensor Status", timeout=60000)
        page.wait_for_timeout(5000)

        # Step 7: Screenshot
        page.screenshot(path=output_path, full_page=True)

        browser.close()

    return output_path
