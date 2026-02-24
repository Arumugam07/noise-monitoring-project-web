#!/usr/bin/env python3
"""
Screenshot Streamlit app after login - searches inside iframes
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

        # Step 2: Debug - list all frames
        frames = page.frames
        print(f"DEBUG: Found {len(frames)} frame(s)")
        for i, frame in enumerate(frames):
            print(f"DEBUG: Frame {i} URL: {frame.url}")
            inputs_in_frame = frame.locator("input").all()
            print(f"DEBUG: Frame {i} has {len(inputs_in_frame)} input(s)")

        # Step 3: Find the frame that has inputs
        login_frame = None
        for frame in page.frames:
            inputs = frame.locator("input").all()
            if len(inputs) > 0:
                login_frame = frame
                print(f"DEBUG: Found inputs in frame: {frame.url}")
                break

        if login_frame is None:
            page.screenshot(path="debug_no_inputs.png", full_page=True)
            raise Exception("Could not find any inputs in any frame")

        # Step 4: Fill login form inside the correct frame
        login_frame.locator("input").nth(0).fill(username)
        login_frame.locator("input[type='password']").fill(password)

        # Step 5: Click Sign In inside the frame
        login_frame.locator("button", has_text="Sign In").click()

        # Step 6: Wait for dashboard to load
        page.wait_for_load_state("networkidle", timeout=60000)
        page.wait_for_timeout(10000)

        # Step 7: Screenshot
        page.screenshot(path=output_path, full_page=True)
        print("DEBUG: Screenshot saved successfully")

        browser.close()

    return output_path
