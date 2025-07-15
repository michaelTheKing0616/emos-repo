# CallFetchFirebaseData/__init__.py
import logging
import requests
import os

def main(name: str) -> str:
    logging.info("Calling FetchFirebaseData HTTP function...")

    try:
        fetch_url = os.environ["FETCH_FIREBASE_URL"]  # Set this in Azure Function App Settings
        response = requests.post(fetch_url)
        response.raise_for_status()
        logging.info(f"FetchFirebaseData response: {response.text}")
        return response.text
    except Exception as e:
        logging.error(f"Failed to call FetchFirebaseData: {e}", exc_info=True)
        raise