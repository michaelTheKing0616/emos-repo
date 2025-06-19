import azure.functions as func
import requests
import json
import os
import psycopg2  # Fixed: removed -binary
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
        logger.error(f"Missing environment variables: ENDPOINT_URL={'present' if endpoint_url else 'missing'}, API_KEY={'present' if api_key else 'missing'}, DATABASE_URL={'present' if db_url else 'missing'}")
        return func.HttpResponse("Missing configuration", status_code=500)

    try:
        req_body = req.get_json()
        
        # Fixed: Create proper test data format that matches score.py expectations
        if req_body and "data" in req_body:
            input_data = req_body
        else:
            # Default test data - simplified format for score.py
            base_datetime = "2025-06-15T00:00:00"
            target_values = [50.5] * 168  # One week of hourly data (minimum for context)
            
            input_data = {
                "data": json.dumps({
                    "datetime": [base_datetime] * len(target_values),  # Will use first datetime
                    "target": target_values
                })
            }
        
        logger.info(f"Using input data keys: {list(input_data.keys())}")
        
    except ValueError as e:
        logger.error(f"Invalid request body: {str(e)}")
        return func.HttpResponse(f"Invalid input: {str(e)}", status_code=400)
    except Exception as e:
        logger.error(f"Error processing request body: {str(e)}")
        return func.HttpResponse(f"Request processing error: {str(e)}", status_code=400)

    headers = {
        "Content-Type": "application/json", 
        "Authorization": f"Bearer {api_key}"
    }
    
    try:
        logger.info(f"Calling endpoint: {endpoint_url}")
        response = requests.post(endpoint_url, json=input_data, headers=headers, timeout=300)
        response.raise_for_status()
        
        # Handle different response formats
        try:
            predictions = response.json()
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON response: {response.text}")
            return func.HttpResponse(f"Invalid response format from endpoint", status_code=500)
            
        logger.info(f"Received predictions type: {type(predictions)}")
        
    except requests.exceptions.Timeout:
        logger.error("Endpoint call timed out")
        return func.HttpResponse("Endpoint timeout", status_code=504)
    except requests.exceptions.RequestException as e:
        logger.error(f"Endpoint call failed: {str(e)}")
        return func.HttpResponse(f"Endpoint error: {str(e)}", status_code=500)

    try:
        # Handle different prediction response formats
        if isinstance(predictions, str):
            predictions = json.loads(predictions)
        
        # Extract forecast data - handle both direct forecast and nested structure
        if "forecast" in predictions:
            prediction_values = predictions["forecast"]
        elif isinstance(predictions, list):
            prediction_values = predictions
        else:
            # Fallback - convert to list if it's a single value or other format
            prediction_values = [predictions] if not isinstance(predictions, list) else predictions
        
        # Create timestamp for storage
        timestamp = datetime.now()
        
        # Store in database
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # Create table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                prediction JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        cur.execute(
            "INSERT INTO predictions (timestamp, prediction) VALUES (%s, %s)", 
            (timestamp, json.dumps(prediction_values))
        )
        conn.commit()
        cur.close()
        conn.close()
        
        logger.info(f"Prediction stored in database: {len(prediction_values) if isinstance(prediction_values, list) else 1} values")
        
    except psycopg2.Error as e:
        logger.error(f"Database error: {str(e)}")
        return func.HttpResponse(f"Database error: {str(e)}", status_code=500)
    except Exception as e:
        logger.error(f"Error processing predictions: {str(e)}")
        return func.HttpResponse(f"Prediction processing error: {str(e)}", status_code=500)

    return func.HttpResponse(
        json.dumps({
            "status": "success", 
            "message": "Predictions stored successfully",
            "prediction_count": len(prediction_values) if isinstance(prediction_values, list) else 1
        }), 
        status_code=200,
        mimetype="application/json"
    )
