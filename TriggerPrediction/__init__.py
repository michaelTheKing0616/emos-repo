import azure.functions as func
import requests
import json
import os
import psycopg2
import numpy as np
from datetime import datetime, timedelta
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def sanitize_iso_timestamp(ts: str) -> str:
    if ts.endswith('+00:00Z'):
        return ts.replace('+00:00Z', '+00:00')
    elif ts.endswith('Z') and '+00:00' in ts:
        return ts.replace('Z', '')
    elif ts.endswith('Z'):
        return ts.replace('Z', '+00:00')
    return ts

def main(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("Function TriggerPrediction started")

    endpoint_url = os.getenv("ENDPOINT_URL")
    api_key = os.getenv("API_KEY")
    db_url = os.getenv("DATABASE_URL")

    if not all([endpoint_url, api_key, db_url]):
        logger.error("Missing environment variables.")
        return func.HttpResponse("Missing configuration", status_code=500)

    input_data = {}
    try:
        try:
            req_body = req.get_json()
        except ValueError:
            logger.warning("Invalid JSON body. Using default test data.")
            req_body = None

        if req_body and "data" in req_body:
            input_data = req_body
        else:
            num_timesteps = 745
            base_datetime = datetime.now() - timedelta(hours=num_timesteps)
            target_values = [50.5 + (np.sin(i / 24 * np.pi) * 10) + (np.random.rand() * 5) for i in range(num_timesteps)]
            dynamic_features_data = [
                [20.0 + (np.sin(i / 24 * np.pi) * 5) for i in range(num_timesteps)],
                [60.0 + (np.cos(i / 48 * np.pi) * 10) for i in range(num_timesteps)],
                [1 if (i % 24 > 7 and i % 24 < 20) else 0 for i in range(num_timesteps)],
                [max(0, 100 * np.sin(i / 24 * np.pi) - 50) for i in range(num_timesteps)],
                [5.0 + (np.random.rand() * 3) for i in range(num_timesteps)],
                [230.0 + (np.sin(i / 12 * np.pi) * 1) for i in range(num_timesteps)],
                [0.5 + (np.random.rand() * 0.5) for i in range(num_timesteps)]
            ]
            input_data = {
                "data": [{
                    "start": base_datetime.isoformat(timespec='seconds') + 'Z',
                    "target": target_values,
                    "feat_dynamic_real": dynamic_features_data,
                    "feat_static_cat": [0],
                    "feat_static_real": [1000.0]
                }]
            }

    except Exception as e:
        logger.error(f"Error preparing input: {e}", exc_info=True)
        return func.HttpResponse(f"Input preparation error: {e}", status_code=400)

    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

        logger.info("Sending request to inference endpoint...")
        response = requests.post(endpoint_url, headers=headers, data=json.dumps(input_data))
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response text (first 500 chars): {response.text[:500]}...")
        response.raise_for_status()

        raw_response = response.text

        try:
            predictions = json.loads(raw_response)
            # 🔁 Handle nested/double JSON string
            if isinstance(predictions, str):
                logger.warning("Endpoint returned JSON string instead of object — attempting to re-parse.")
                predictions = json.loads(predictions)
        except json.JSONDecodeError as json_e:
            logger.error(f"Failed to parse endpoint response: {json_e}. Raw: {raw_response}")
            return func.HttpResponse(f"Error decoding response: {json_e}", status_code=500)

        if isinstance(predictions, dict):
            if "error" in predictions:
                logger.error(f"Endpoint error: {predictions['error']}")
                return func.HttpResponse(f"Endpoint error: {predictions['error']}", status_code=500)
            elif "predictions" in predictions:
                prediction_values = predictions["predictions"]
            else:
                logger.error("Unexpected dict structure in prediction response.")
                return func.HttpResponse("Unexpected prediction format", status_code=500)
        elif isinstance(predictions, list):
            prediction_values = predictions
        else:
            logger.error(f"Unexpected type: {type(predictions)}")
            return func.HttpResponse("Invalid prediction response type", status_code=500)

        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                prediction JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        for item in prediction_values:
            item_timestamp_str = item.get("start")
            if item_timestamp_str:
                try:
                    clean_ts = sanitize_iso_timestamp(item_timestamp_str)
                    item_timestamp = datetime.fromisoformat(clean_ts)
                except Exception as ts_err:
                    logger.warning(f"Failed to parse timestamp '{item_timestamp_str}': {ts_err}")
                    item_timestamp = datetime.utcnow()
            else:
                item_timestamp = datetime.utcnow()

            cur.execute(
                "INSERT INTO predictions (timestamp, prediction) VALUES (%s, %s)",
                (item_timestamp, json.dumps(item))
            )

        conn.commit()
        cur.close()
        conn.close()

        logger.info("Predictions stored in database.")

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error: {http_err}. Response: {response.text}", exc_info=True)
        return func.HttpResponse(f"HTTP error: {http_err}", status_code=response.status_code)
    except requests.exceptions.RequestException as req_err:
        logger.error(f"Request exception: {req_err}", exc_info=True)
        return func.HttpResponse(f"Request error: {req_err}", status_code=500)
    except psycopg2.Error as db_err:
        logger.error(f"Database error: {db_err}", exc_info=True)
        return func.HttpResponse(f"Database error: {db_err}", status_code=500)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse(f"Unexpected error: {e}", status_code=500)

    return func.HttpResponse(
        json.dumps({
            "status": "success",
            "message": "Predictions stored successfully",
            "prediction_count": len(prediction_values) if prediction_values else 0
        }),
        mimetype="application/json"
    )
