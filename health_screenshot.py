#!/usr/bin/env python3
"""
Screenshot the actual Streamlit app system health page.
"""

import os
import logging
from playwright.sync_api import sync_playwright

log = logging.getLogger("health-screenshot")

STREAMLIT_URL = "https://noise-monitoring-project-web.streamlit.app"
APP_USERNAME = os.getenv("APP_USERNAME", "afic")
APP_PASSWORD = os.getenv("APP_PASSWORD", "Password3!")


def screenshot_streamlit_health(output_path: str = "health_alert.png") -> str:
    """Log into Streamlit app and screenshot the sensor health section."""
    
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1400, "height": 900})
        
        log.info(f"Navigating to {STREAMLIT_URL}")
        page.goto(STREAMLIT_URL, wait_until="networkidle", timeout=60000)
        
        # Wait for login form
        page.wait_for_selector("input[type='text']", timeout=30000)
        log.info("Login form found, logging in...")
        
        # Fill username and password
        page.fill("input[type='text']", APP_USERNAME)
        page.fill("input[type='password']", APP_PASSWORD)
        
        # Click sign in button
        page.get_by_text("Sign In").click()
        
        # Wait for dashboard to load
        page.wait_for_selector("text=Sensor Health", timeout=30000)
        page.wait_for_timeout(3000)  # Extra wait for cards to render
        log.info("Dashboard loaded")
        
        # Scroll to sensor health section
        page.evaluate("""
            const el = Array.from(document.querySelectorAll('*'))
                .find(e => e.innerText && e.innerText.includes('Sensor Health'));
            if (el) el.scrollIntoView();
        """)
        page.wait_for_timeout(1000)
        
        # Take full page screenshot
        page.screenshot(path=output_path, full_page=True)
        log.info(f"Screenshot saved: {output_path}")
        
        browser.close()
    
    return output_path
