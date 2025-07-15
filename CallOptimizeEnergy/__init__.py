# CallOptimizeEnergy/__init__.py
import logging
import requests
import os

def main(input_data: str) -> str:
    logging.info("Calling OptimizeEnergy HTTP function...")

    try:
        optimize_url = os.environ["OPTIMIZE_ENERGY_URL"]
        response = requests.post(optimize_url, json={"input": input_data})
        response.raise_for_status()
        logging.info(f"OptimizeEnergy response: {response.text}")
        return response.text
    except Exception as e:
        logging.error(f"Failed to call OptimizeEnergy: {e}", exc_info=True)
        raise