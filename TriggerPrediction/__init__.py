import azure.functions as func
import requests
import json
import os
import psycopg2
import numpy as np  # Added for default data generation
from datetime import datetime, timedelta  # Modified to include timedelta
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

    input_data = {} # Initialize input_data
    try:
        req_body = None
        try:
            req_body = req.get_json()
        except ValueError:
            logger.warning("Request body is not valid JSON. Attempting to use default data.")
        
        if req_body and "data" in req_body:
            input_data = req_body
            logger.info("Using input data from request body.")
        else:
            logger.info("No 'data' key in request body or invalid JSON. Generating default test data.")
            # Default test data generation for 745 timesteps, matching model's context_length
            num_timesteps = 745
            # Use current time minus history length for a realistic 'start'
            base_datetime = datetime.now() - timedelta(hours=num_timesteps) 
            target_values = [50.5 + (np.sin(i / 24 * np.pi) * 10) + (np.random.rand() * 5) for i in range(num_timesteps)]
            
            dynamic_features_data = [
                [20.0 + (np.sin(i / 24 * np.pi) * 5) for i in range(num_timesteps)],         # temperature
                [60.0 + (np.cos(i / 48 * np.pi) * 10) for i in range(num_timesteps)],        # humidity
                [1 if (i % 24 > 7 and i % 24 < 20) else 0 for i in range(num_timesteps)],    # occupancy
                [max(0, 100 * np.sin(i / 24 * np.pi) - 50) for i in range(num_timesteps)],   # solar_irradiance
                [5.0 + (np.random.rand() * 3) for i in range(num_timesteps)],                # wind_speed
                [230.0 + (np.sin(i / 12 * np.pi) * 1) for i in range(num_timesteps)],        # voltage
                [0.5 + (np.random.rand() * 0.5) for i in range(num_timesteps)]               # current
            ]
            
            input_data = {
                "data": [{
                    "start": base_datetime.isoformat(timespec='seconds') + 'Z', # ISO format with UTC indicator
                    "target": target_values,
                    "feat_dynamic_real": dynamic_features_data,
                    "feat_static_cat": [0], # Example static categorical feature
                    "feat_static_real": [1000.0] # Example static real feature
                }]
            }
            logger.info("Default test data generated successfully.")
            
    except Exception as e:
        logger.error(f"Error processing request body or generating default data: {str(e)}", exc_info=True)
        return func.HttpResponse(f"Invalid request body or data generation error: {str(e)}", status_code=400)

    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        logger.info(f"Sending data to endpoint: {endpoint_url}")
        # Ensure the payload is a JSON string before sending
        response = requests.post(endpoint_url, headers=headers, data=json.dumps(input_data))
        
        # Log response details for debugging before attempting to parse
        logger.info(f"Endpoint response status: {response.status_code}")
        logger.info(f"Endpoint response text (first 500 chars): {response.text[:500]}...")

        response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
        
        predictions = None
        try:
            predictions = response.json()
            # Verify that the parsed object is what we expect (a dict or a list)
            if not isinstance(predictions, (dict, list)):
                raise ValueError(f"Endpoint response.json() did not return expected dict/list, but {type(predictions).__name__}")
            
            if isinstance(predictions, dict) and "predictions" in predictions:
                logger.info(f"Successfully received response from endpoint. Keys: {list(predictions.keys())}. Number of predictions returned: {len(predictions.get('predictions', []))}")
            else:
                logger.info(f"Successfully received response from endpoint. Response is a list or unexpected dict structure.")
                
        except json.JSONDecodeError as json_e:
            logger.error(f"Failed to decode JSON from endpoint response: {json_e}. Raw response: {response.text}")
            return func.HttpResponse(f"Error decoding prediction response: {json_e}. Raw response: {response.text}", status_code=500)
        except ValueError as val_e:
            logger.error(f"Invalid format after JSON decode: {val_e}. Raw response: {response.text}")
            return func.HttpResponse(f"Invalid prediction response format: {val_e}. Raw response: {response.text}", status_code=500)
        
        # Extract the list of prediction items
        prediction_values = []
        if isinstance(predictions, dict) and "predictions" in predictions:
            prediction_values = predictions["predictions"]
        elif isinstance(predictions, list): # Fallback if endpoint directly returns a list of predictions
            prediction_values = predictions
        else:
            logger.error(f"Unexpected prediction format from endpoint: {predictions}. Cannot extract values for database.")
            return func.HttpResponse("Unexpected prediction response format from endpoint.", status_code=500)

        # Database insertion logic
        logger.info("Attempting to connect to database...")
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        
        # Create table if it doesn't exist (ensure this DDL matches your requirements)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                prediction JSONB NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert each prediction item as a separate record
        if prediction_values:
            for item in prediction_values:
                # Extract timestamp from the prediction item itself if available, otherwise use a fallback
                item_timestamp_str = item.get("start")
                # Ensure the timestamp is in a format psycopg2 can handle (remove 'Z' and let fromisoformat handle UTC)
                item_timestamp = datetime.fromisoformat(item_timestamp_str.replace('Z', '+00:00')) if item_timestamp_str else datetime.utcnow()
                
                cur.execute(
                    "INSERT INTO predictions (timestamp, prediction) VALUES (%s, %s)", 
                    (item_timestamp, json.dumps(item)) # Store the entire prediction item as JSONB
                )
            logger.info(f"Stored {len(prediction_values)} individual predictions in database.")
        else:
            logger.warning("No predictions received from endpoint to store in database.")

        conn.commit()
        cur.close()
        conn.close()
        
        logger.info("Prediction storage process completed.")
        
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while calling endpoint: {http_err}. Response: {response.text}", exc_info=True)
        return func.HttpResponse(f"Endpoint HTTP error: {http_err}. Response: {response.text}", status_code=response.status_code)
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Connection error while calling endpoint: {conn_err}", exc_info=True)
        return func.HttpResponse(f"Endpoint connection error: {conn_err}", status_code=503)
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Timeout error while calling endpoint: {timeout_err}", exc_info=True)
        return func.HttpResponse(f"Endpoint timeout error: {timeout_err}", status_code=504)
    except requests.exceptions.RequestException as req_err:
        logger.error(f"General request error while calling endpoint: {req_err}", exc_info=True)
        return func.HttpResponse(f"Error calling endpoint: {req_err}", status_code=500)
    except psycopg2.Error as e:
        logger.error(f"Database error: {str(e)}", exc_info=True)
        return func.HttpResponse(f"Database error: {str(e)}", status_code=500)
    except Exception as e:
        logger.error(f"An unexpected error occurred during prediction processing or database insertion: {str(e)}", exc_info=True)
        return func.HttpResponse(f"Prediction processing error: {str(e)}", status_code=500)

    # Final successful response
    return func.HttpResponse(
        json.dumps({
            "status": "success", 
            "message": "Predictions stored successfully",
            "prediction_count": len(prediction_values)
        }),
        mimetype="application/json"
    )
