#!/usr/bin/env python3
"""
Screenshot Streamlit app after login - with debug screenshot
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
        page.wait_for_timeout(8000)

        # Step 2: Debug - take screenshot of whatever is showing right now
        page.screenshot(path="debug_before_login.png", full_page=True)
        print("DEBUG: Saved debug_before_login.png — check what page is showing")
        print(f"DEBUG: Current URL: {page.url}")
        print(f"DEBUG: Page title: {page.title()}")

        # Step 3: Try clicking any wake-up button (try multiple possible texts)
        wake_texts = [
            "Yes, get this app back up!",
            "Get this app back up",
            "Wake up",
            "Yes",
        ]
        for text in wake_texts:
            try:
                btn = page.get_by_text(text, exact=False)
                if btn.is_visible():
                    print(f"DEBUG: Found wake button with text: '{text}' — clicking")
                    btn.click()
                    page.wait_for_timeout(20000)
                    page.screenshot(path="debug_after_wake.png", full_page=True)
                    print("DEBUG: Saved debug_after_wake.png")
                    break
            except Exception as e:
                print(f"DEBUG: Button '{text}' not found: {e}")

        # Step 4: Wait for login form
        try:
            page.get_by_placeholder("Enter your username").wait_for(
                state="visible",
                timeout=60000
            )
        except Exception:
            # Take one more debug screenshot to see what went wrong
            page.screenshot(path="debug_login_timeout.png", full_page=True)
            print("DEBUG: Saved debug_login_timeout.png — login form never appeared")
            raise

        # Step 5: Fill in login form
        page.get_by_placeholder("Enter your username").fill(username)
        page.get_by_placeholder("Enter your password").fill(password)
        page.get_by_text("Sign In").click()

        # Step 6: Wait for dashboard
        page.wait_for_selector("text=Sensor Status", timeout=60000)
        page.wait_for_timeout(5000)

        # Step 7: Final screenshot
        page.screenshot(path=output_path, full_page=True)
        browser.close()

    return output_path
