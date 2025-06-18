import azure.functions as func
import requests
import json
import os
import psycopg2
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("Function TriggerPrediction started")
    
    endpoint_url = os.getenv("ENDPOINT_URL")
    api_key = os.getenv("API_KEY")
    db_url = os.getenv("DATABASE_URL")
    
    if not all([endpoint_url, api_key, db_url]):
        logger.error(f"Missing environment variables: ENDPOINT_URL={endpoint_url}, API_KEY={api_key[:5]}..., DATABASE_URL={db_url}")
        return func.HttpResponse("Missing configuration", status_code=500)

    try:
        req_body = req.get_json()
        input_data = req_body if req_body and "data" in req_body else {
            "data": [{
                "start": "2025-06-15T00:00:00Z",
                "target": [50.5] * 24,
                "feat_dynamic_real": [[22.3] * 24, [55.0] * 24, [1] * 24, [10.5] * 24, [60.0] * 24, [2300.0] * 24, [0.95] * 24, [220.0] * 24],
                "feat_static_cat": [0],
                "feat_static_real": [1000.0]
            }]
        }
        logger.info(f"Using input data: {json.dumps(input_data)}")
    except ValueError as e:
        logger.error(f"Invalid request body: {str(e)}")
        return func.HttpResponse(f"Invalid input: {str(e)}", status_code=400)

    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {api_key}"}
    try:
        logger.info(f"Calling endpoint: {endpoint_url}")
        response = requests.post(endpoint_url, json=input_data, headers=headers)
        response.raise_for_status()
        predictions = response.json()
        logger.info(f"Received predictions: {json.dumps(predictions)}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Endpoint call failed: {str(e)}")
        return func.HttpResponse(f"Endpoint error: {str(e)}", status_code=500)

    try:
        prediction = predictions["predictions"][0]["mean"]
        timestamp = datetime.strptime(predictions["predictions"][0]["start"], "%Y-%m-%dT%H:%M:%S%z")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("INSERT INTO predictions (timestamp, prediction) VALUES (%s, %s)", (timestamp, json.dumps(prediction)))
        conn.commit()
        cur.close()
        conn.close()
        logger.info("Prediction stored in database")
    except Exception as e:
        logger.error(f"Database error: {str(e)}")
        return func.HttpResponse(f"Database error: {str(e)}", status_code=500)

    return func.HttpResponse("Predictions stored", status_code=200)
