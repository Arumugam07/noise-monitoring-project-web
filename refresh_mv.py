#!/usr/bin/env python3
"""Refresh the Aiven dashboard after ingestion; fail visibly on database errors."""
import logging
from dotenv import load_dotenv
from database import create_client

logging.basicConfig(level=logging.INFO)


def main():
    load_dotenv()
    client = create_client()
    client.refresh()
    logging.info("Aiven dashboard refreshed: %s", client.date_range().data)


if __name__ == "__main__":
    main()
