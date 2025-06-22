import azure.functions as func
import requests
import json
import os
import psycopg2
import numpy as np
from datetime import datetime, timedelta
import logging
from dateutil import parser

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

    input_data = {}
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
        response = requests.post(endpoint_url, headers=headers, data=json.dumps(input_data))
        
        logger.info(f"Endpoint response status: {response.status_code}")
        logger.info(f"Endpoint response text (first 500 chars): {response.text[:500]}...")
        response.raise_for_status()
        
        predictions = None
        try:
            predictions = response.json()

            # Handle case where the response is double-encoded JSON
            if isinstance(predictions, str):
                try:
                    predictions = json.loads(predictions)
                    logger.info("Successfully decoded double-encoded JSON from endpoint.")
                except json.JSONDecodeError as decode_error:
                    logger.error(f"Failed to decode stringified JSON: {decode_error}. Raw string: {predictions}")
                    return func.HttpResponse(f"Error decoding nested JSON: {decode_error}", status_code=500)

            if isinstance(predictions, dict):
                if "error" in predictions:
                    logger.error(f"Endpoint returned an error: {predictions['error']}")
                    return func.HttpResponse(f"Endpoint error: {predictions['error']}", status_code=500)
                elif "predictions" in predictions:
                    prediction_values = predictions["predictions"]
                    logger.info(f"Successfully received {len(prediction_values)} predictions from endpoint.")
                else:
                    logger.error(f"Unexpected dict response format: {predictions}")
                    return func.HttpResponse("Unexpected endpoint response format", status_code=500)
            elif isinstance(predictions, list):
                prediction_values = predictions
                logger.info(f"Successfully received {len(prediction_values)} predictions as a list from endpoint.")
            else:
                logger.error(f"Unexpected response type: {type(predictions)}. Response: {predictions}")
                return func.HttpResponse("Invalid endpoint response type", status_code=500)
                
        except json.JSONDecodeError as json_e:
            logger.error(f"Failed to decode JSON from endpoint response: {json_e}. Raw response: {response.text}")
            return func.HttpResponse(f"Error decoding prediction response: {json_e}. Raw response: {response.text}", status_code=500)
        except Exception as e:
            logger.error(f"Error processing endpoint response: {e}", exc_info=True)
            return func.HttpResponse(f"Error processing response: {e}", status_code=500)
        
        # Database insertion logic
        logger.info("Attempting to connect to database...")
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
        
        if prediction_values:
            for item in prediction_values:
                item_timestamp_str = item.get("start")
                
                item_timestamp = parser.isoparse(item_timestamp_str)

                    if item_timestamp_str
                    else datetime.utcnow()
                )
                
                cur.execute(
                    "INSERT INTO predictions (timestamp, prediction) VALUES (%s, %s)",
                    (item_timestamp, json.dumps(item))
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

    return func.HttpResponse(
        json.dumps({
            "status": "success",
            "message": "Predictions stored successfully",
            "prediction_count": len(prediction_values) if prediction_values else 0
        }),
        mimetype="application/json"
    )
