#!/usr/bin/env python3
"""
Screenshot Streamlit app after login - full render wait + clean capture
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
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--force-device-scale-factor=1"
            ]
        )

        # High resolution viewport for crisp screenshot
        page = browser.new_page(
            viewport={"width": 1600, "height": 900},
            device_scale_factor=2  # Retina quality
        )

        # Step 1: Load the app
        print("DEBUG: Loading app...")
        page.goto(STREAMLIT_URL, timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)
        page.wait_for_timeout(5000)

        # Step 2: Find frame with inputs (Streamlit uses iframes)
        print("DEBUG: Looking for login form...")
        login_frame = None
        for frame in page.frames:
            inputs = frame.locator("input").all()
            if len(inputs) > 0:
                login_frame = frame
                print(f"DEBUG: Found login frame: {frame.url}")
                break

        if login_frame is None:
            page.screenshot(path="debug_no_frame.png", full_page=True)
            raise Exception("Could not find login form in any frame")

        # Step 3: Fill login form
        print("DEBUG: Logging in...")
        login_frame.locator("input").nth(0).fill(username)
        login_frame.locator("input[type='password']").fill(password)
        login_frame.locator("button", has_text="Sign In").click()

        # Step 4: Wait for loading spinner to disappear
        print("DEBUG: Waiting for data to load...")
        page.wait_for_timeout(5000)
        try:
            page.wait_for_selector(
                "text=Loading all data from database",
                state="detached",
                timeout=60000
            )
            print("DEBUG: Data loaded")
        except Exception:
            print("DEBUG: Spinner wait timed out — proceeding")

        # Step 5: Wait for sensor health cards to appear
        try:
            page.wait_for_selector(
                "text=System Health",
                state="visible",
                timeout=60000
            )
            print("DEBUG: Sensor cards visible")
        except Exception:
            print("DEBUG: Could not confirm sensor cards — proceeding")

        # Step 6: Extra buffer for ALL 13 cards to fully render
        page.wait_for_timeout(8000)

        # Step 7: Scroll through the full page to trigger lazy rendering
        print("DEBUG: Scrolling page to ensure all cards render...")
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)
        page.evaluate("window.scrollTo(0, 0)")  # Scroll back to top
        page.wait_for_timeout(2000)

        # Step 8: Hide Streamlit's menu bar and footer for cleaner screenshot
        page.evaluate("""
            () => {
                // Hide Streamlit top toolbar
                const toolbar = document.querySelector('header');
                if (toolbar) toolbar.style.display = 'none';
                
                // Hide Streamlit footer
                const footer = document.querySelector('footer');
                if (footer) footer.style.display = 'none';

                // Hide deploy button area
                const deploy = document.querySelector('.stDeployButton');
                if (deploy) deploy.style.display = 'none';
            }
        """)

        # Step 9: Take full page screenshot
        print("DEBUG: Taking screenshot...")
        page.screenshot(
            path=output_path,
            full_page=True,
            type="png"
        )
        print(f"DEBUG: Screenshot saved to {output_path}")

        browser.close()

    return output_path
