#!/usr/bin/env python3
"""
Screenshot Streamlit app - forces sidebar collapse, full page capture
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
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )

        # Wide viewport so content isn't cramped
        page = browser.new_page(
            viewport={"width": 1600, "height": 900},
            device_scale_factor=1
        )

        # Step 1: Load app
        print("DEBUG: Loading app...")
        page.goto(STREAMLIT_URL, timeout=60000)
        page.wait_for_load_state("networkidle", timeout=60000)
        page.wait_for_timeout(5000)

        # Step 2: Find login frame
        print("DEBUG: Looking for login form...")
        login_frame = None
        for frame in page.frames:
            inputs = frame.locator("input").all()
            if len(inputs) > 0:
                login_frame = frame
                break

        if login_frame is None:
            page.screenshot(path="debug_no_frame.png", full_page=True)
            raise Exception("Could not find login form in any frame")

        # Step 3: Login
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

        # Step 5: Wait for sensor cards
        try:
            page.wait_for_selector(
                "text=System Health",
                state="visible",
                timeout=60000
            )
        except Exception:
            print("DEBUG: Could not confirm sensor cards")

        # Step 6: Buffer for all cards to render
        page.wait_for_timeout(8000)

        # Step 7: Scroll full page to trigger lazy render
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(2000)

        # Step 8: Force close sidebar by clicking the collapse button if visible
        try:
            # Try clicking the sidebar close button directly
            close_btn = page.locator('[data-testid="baseButton-headerNoPadding"]').first
            if close_btn.is_visible():
                close_btn.click()
                page.wait_for_timeout(1000)
                print("DEBUG: Clicked sidebar close button")
        except Exception:
            pass

        # Step 9: Aggressively hide all Streamlit chrome via CSS injection
        page.add_style_tag(content="""
            [data-testid="stSidebar"] { display: none !important; }
            [data-testid="collapsedControl"] { display: none !important; }
            [data-testid="stHeader"] { display: none !important; }
            [data-testid="stFooter"] { display: none !important; }
            [data-testid="stToolbar"] { display: none !important; }
            [data-testid="stDecoration"] { display: none !important; }
            .stDeployButton { display: none !important; }
            .viewerBadge_container__1QSob { display: none !important; }
            a[href*="streamlit.io"] { display: none !important; }
            header { display: none !important; }
            footer { display: none !important; }
            section[data-testid="stSidebar"] { display: none !important; }
            .appview-container { margin-left: 0 !important; }
            .main .block-container {
                max-width: 100% !important;
                padding-left: 3rem !important;
                padding-right: 3rem !important;
                padding-top: 2rem !important;
            }
        """)

        page.wait_for_timeout(1500)

        # Step 10: Full page screenshot — automatically stitches entire page
        print("DEBUG: Taking full page screenshot...")
        page.screenshot(
            path=output_path,
            full_page=True,
            type="png"
        )
        print(f"DEBUG: Saved to {output_path}")

        browser.close()

    return output_path
