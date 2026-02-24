#!/usr/bin/env python3
"""
Screenshot Streamlit app - injects CSS before render to hide sidebar
"""
import os
from playwright.sync_api import sync_playwright

STREAMLIT_URL = "https://noise-monitoring-project-web.streamlit.app"

# CSS to hide all Streamlit chrome — injected early and repeatedly
HIDE_CHROME_CSS = """
    [data-testid="stSidebar"],
    [data-testid="collapsedControl"],
    [data-testid="stHeader"],
    [data-testid="stFooter"],
    [data-testid="stToolbar"],
    [data-testid="stDecoration"],
    [data-testid="stStatusWidget"],
    .stDeployButton,
    header, footer,
    a[href*="streamlit.io"] {
        display: none !important;
        visibility: hidden !important;
        width: 0 !important;
        height: 0 !important;
    }
    .appview-container {
        margin-left: 0 !important;
    }
    .main .block-container {
        max-width: 100% !important;
        padding-left: 3rem !important;
        padding-right: 3rem !important;
        padding-top: 1rem !important;
    }
"""

def screenshot_streamlit_health(output_path="health_alert.png"):
    
    username = os.getenv("APP_USERNAME", "afic")
    password = os.getenv("APP_PASSWORD", "Password3!")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )

        page = browser.new_page(
            viewport={"width": 1600, "height": 900},
            device_scale_factor=1
        )

        # Inject CSS on every new document load — catches Streamlit re-renders
        page.add_init_script(f"""
            const style = document.createElement('style');
            style.textContent = `{HIDE_CHROME_CSS}`;
            document.head.appendChild(style);
            
            // Re-inject on every DOM mutation (Streamlit re-renders constantly)
            const observer = new MutationObserver(() => {{
                if (!document.getElementById('hide-chrome-style')) {{
                    const s = document.createElement('style');
                    s.id = 'hide-chrome-style';
                    s.textContent = `{HIDE_CHROME_CSS}`;
                    document.head.appendChild(s);
                }}
            }});
            observer.observe(document.documentElement, {{ childList: true, subtree: true }});
        """)

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

        # Step 6: Buffer for all 13 cards
        page.wait_for_timeout(8000)

        # Step 7: Scroll to trigger lazy render
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(2000)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(2000)

        # Step 8: Re-apply CSS one final time before screenshot
        page.add_style_tag(content=HIDE_CHROME_CSS)
        page.wait_for_timeout(2000)

        # Step 9: Full page screenshot
        print("DEBUG: Taking screenshot...")
        page.screenshot(
            path=output_path,
            full_page=True,
            type="png"
        )
        print(f"DEBUG: Saved to {output_path}")

        browser.close()

    return output_path
