# CallTriggerPrediction/__init__.py
import logging
import requests
import os

def main(input_data: str) -> str:
    logging.info("Calling TriggerPrediction HTTP function...")

    try:
        trigger_url = os.environ["TRIGGER_PREDICTION_URL"]
        response = requests.post(trigger_url, json={"input": input_data})
        response.raise_for_status()
        logging.info(f"TriggerPrediction response: {response.text}")
        return response.text
    except Exception as e:
        logging.error(f"Failed to call TriggerPrediction: {e}", exc_info=True)
        raise