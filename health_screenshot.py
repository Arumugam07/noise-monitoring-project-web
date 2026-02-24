#!/usr/bin/env python3
"""
Screenshot Streamlit app - one clean full page capture
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

        page = browser.new_page(
            viewport={"width": 1400, "height": 900},
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
            print("DEBUG: Sensor cards visible")
        except Exception:
            print("DEBUG: Could not confirm sensor cards")

        # Step 6: Buffer for all 13 cards to fully render
        page.wait_for_timeout(8000)

        # Step 7: Scroll to trigger lazy render then back to top
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(2000)

        # Step 8: Hide all Streamlit chrome elements
        print("DEBUG: Cleaning up UI...")
        page.evaluate("""
            () => {
                // Hide sidebar
                const sidebar = document.querySelector('[data-testid="stSidebar"]');
                if (sidebar) sidebar.style.display = 'none';

                // Hide sidebar collapse arrow button
                const collapseBtn = document.querySelector('[data-testid="collapsedControl"]');
                if (collapseBtn) collapseBtn.style.display = 'none';

                // Hide top header bar (contains Fork, GitHub, menu buttons)
                const header = document.querySelector('[data-testid="stHeader"]');
                if (header) header.style.display = 'none';

                // Hide footer
                const footer = document.querySelector('[data-testid="stFooter"]');
                if (footer) footer.style.display = 'none';

                // Hide Streamlit toolbar (top right buttons)
                const toolbar = document.querySelector('[data-testid="stToolbar"]');
                if (toolbar) toolbar.style.display = 'none';

                // Hide ALL Streamlit watermarks and badges
                document.querySelectorAll(
                    'a[href*="streamlit.io"], .viewerBadge_container__1QSob, ' +
                    '[data-testid="stDecoration"], .stDeployButton'
                ).forEach(el => el.style.display = 'none');

                // Expand main content to use full width now sidebar is gone
                const appView = document.querySelector('.appview-container');
                if (appView) appView.style.marginLeft = '0';

                const mainContent = document.querySelector('.main');
                if (mainContent) mainContent.style.paddingLeft = '2rem';

                // Remove top padding gap left by hidden header
                const block = document.querySelector('[data-testid="block-container"]');
                if (block) {
                    block.style.paddingTop = '2rem';
                    block.style.maxWidth = '100%';
                }
            }
        """)

        page.wait_for_timeout(1000)

        # Step 9: full_page=True captures ENTIRE page as ONE image automatically
        print("DEBUG: Taking full page screenshot...")
        page.screenshot(
            path=output_path,
            full_page=True,
            type="png"
        )
        print(f"DEBUG: Done — saved to {output_path}")

        browser.close()

    return output_path
