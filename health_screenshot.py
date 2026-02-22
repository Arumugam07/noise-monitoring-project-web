#!/usr/bin/env python3
"""
Generate a health status screenshot for Telegram alerts.
Uses playwright to render HTML → PNG.
"""

import os
from datetime import date
from playwright.sync_api import sync_playwright


def generate_health_html(offline_locations: list, health_df, report_date: date) -> str:
    """Generate HTML for the health alert screenshot."""
    
    rows_html = ""
    for _, row in health_df.iterrows():
        status = row['Status']
        color = {"ONLINE": "#28a745", "DEGRADED": "#ffc107", "OFFLINE": "#dc3545"}[status]
        icon = {"ONLINE": "✅", "DEGRADED": "⚠️", "OFFLINE": "❌"}[status]
        rows_html += f"""
        <tr style="background: {'#f8d7da' if status == 'OFFLINE' else '#fff3cd' if status == 'DEGRADED' else '#d4edda'}">
            <td style="padding: 10px; border: 1px solid #ddd;">{row['Location']}</td>
            <td style="padding: 10px; border: 1px solid #ddd; color: {color}; font-weight: bold;">
                {icon} {status}
            </td>
            <td style="padding: 10px; border: 1px solid #ddd;">{row['Completeness_%']:.1f}%</td>
            <td style="padding: 10px; border: 1px solid #ddd;">{row['Days_Online']}/{row['Total_Days']}</td>
            <td style="padding: 10px; border: 1px solid #ddd;">{int(row['Total_Readings']):,}/{int(row['Expected_Readings']):,}</td>
        </tr>
        """

    offline_alert_html = ""
    if offline_locations:
        items = "".join([
            f"<li><strong>{a['location_name']}</strong>: {a['offline_start']} → {a['offline_end']} ({a['consecutive_days']} days)</li>"
            for a in offline_locations
        ])
        offline_alert_html = f"""
        <div style="background: #f8d7da; border-left: 5px solid #dc3545; padding: 15px; 
                    border-radius: 8px; margin-bottom: 20px;">
            <h3 style="color: #721c24; margin: 0 0 10px 0;">
                🚨 CRITICAL: {len(offline_locations)} Location(s) Offline 7+ Consecutive Days
            </h3>
            <ul style="margin: 0; color: #721c24;">
                {items}
            </ul>
        </div>
        """

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
                margin: 0; padding: 20px;
                background: #f5f5f5;
                width: 900px;
            }}
            .header {{
                background: linear-gradient(135deg, #667eea, #764ba2);
                color: white; padding: 20px; border-radius: 10px;
                margin-bottom: 20px;
            }}
            table {{
                width: 100%; border-collapse: collapse;
                background: white; border-radius: 8px;
                overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            th {{
                background: #667eea; color: white;
                padding: 12px; text-align: left;
                border: 1px solid #ddd;
            }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2 style="margin: 0;">🔊 RSAF Noise Monitoring System</h2>
            <p style="margin: 5px 0 0 0; opacity: 0.9;">
                Monthly Health Alert — {report_date.strftime('%B %Y')}
            </p>
            <p style="margin: 3px 0 0 0; opacity: 0.8; font-size: 0.9rem;">
                Generated: {date.today().strftime('%Y-%m-%d')}
            </p>
        </div>

        {offline_alert_html}

        <table>
            <thead>
                <tr>
                    <th>Location</th>
                    <th>Status</th>
                    <th>Completeness</th>
                    <th>Days Online</th>
                    <th>Readings</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </body>
    </html>
    """
    return html


def screenshot_health_report(html_content: str, output_path: str = "health_alert.png"):
    """Render HTML to PNG using Playwright."""
    html_file = "/tmp/health_alert.html"
    with open(html_file, "w") as f:
        f.write(html_content)

    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 940, "height": 800})
        page.goto(f"file://{html_file}")
        page.wait_for_timeout(500)

        # Auto-height screenshot
        height = page.evaluate("document.body.scrollHeight")
        page.set_viewport_size({"width": 940, "height": height + 40})
        page.screenshot(path=output_path, full_page=True)
        browser.close()

    return output_path
