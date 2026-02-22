#!/usr/bin/env python3
"""
Send Telegram alert with screenshot for offline sensor notifications.
"""

import os
import requests
import logging

log = logging.getLogger("telegram-alert")


def send_telegram_message(message: str, token: str, chat_id: str):
    """Send a text message via Telegram."""
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML"
    }
    resp = requests.post(url, json=payload, timeout=30)
    if resp.status_code == 200:
        log.info("✅ Telegram message sent")
    else:
        log.error(f"❌ Failed to send message: {resp.text}")
    return resp


def send_telegram_photo(image_path: str, caption: str, token: str, chat_id: str):
    """Send a photo with caption via Telegram."""
    url = f"https://api.telegram.org/bot{token}/sendPhoto"
    with open(image_path, "rb") as img:
        resp = requests.post(
            url,
            data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
            files={"photo": img},
            timeout=30
        )
    if resp.status_code == 200:
        log.info("✅ Telegram photo sent")
    else:
        log.error(f"❌ Failed to send photo: {resp.text}")
    return resp


def send_telegram_document(file_path: str, caption: str, token: str, chat_id: str):
    """Send a document/file via Telegram."""
    url = f"https://api.telegram.org/bot{token}/sendDocument"
    with open(file_path, "rb") as f:
        resp = requests.post(
            url,
            data={"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"},
            files={"document": f},
            timeout=30
        )
    if resp.status_code == 200:
        log.info("✅ Telegram document sent")
    else:
        log.error(f"❌ Failed to send document: {resp.text}")
    return resp
