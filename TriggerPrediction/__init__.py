import azure.functions as func
import requests
import json
import os
import psycopg2
from datetime import datetime, timedelta
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
        logger.info(f"Received request body: {req_body}")

        # The score.py expects a dictionary with a "data" key, where the value is a list of series objects.
        # Or, if not wrapped, a single series object or a list of series objects directly.
        # Let's ensure the input_data_for_scoring is always formatted as score.py expects:
        # {"data": [ {series1}, {series2}, ... ]}
        
        input_data_for_scoring_list = []
        
        # If the request body provides data, use it. Otherwise, use default test data.
        if req_body and ("data" in req_body or ("start" in req_body and "target" in req_body)):
            # If "data" key exists, use its value (which should be a list of series objects)
            if "data" in req_body:
                input_data_for_scoring_list = req_body["data"]
            else:
                # If not wrapped, assume it's a single series object and wrap it in a list
                input_data_for_scoring_list = [req_body]
            logger.info("Using data from request body.")
        else:
            # Default test data generation for a single series
            logger.info("No valid input in request, generating default test data.")
            # Your model's context_length is 745. Provide at least this many data points.
            num_timesteps = 745 
            base_datetime = datetime.now() - timedelta(hours=num_timesteps) # Start X hours ago

            target_values = [50.5 + (i % 20) + (i / 100) for i in range(num_timesteps)] # Simulate energy

            dynamic_features_data = {
                "temperature": [20.0 + (i % 5) for i in range(num_timesteps)],
                "humidity": [60.0 + (i % 10) for i in range(num_timesteps)],
                "occupancy": [1 if i % 24 < 12 else 0 for i in range(num_timesteps)], # Simulate day/night occupancy
                "solar_irradiance": [100.0 + (i % 100) for i in range(num_timesteps)],
                "wind_speed": [5.0 + (i % 5) for i in range(num_timesteps)],
                "voltage": [230.0 + (i % 2) for i in range(num_timesteps)],
                "current": [0.5 + (i % 1) for i in range(num_timesteps)]
            }

            datetime_list = [(base_datetime + timedelta(hours=i)).isoformat(timespec='seconds') + 'Z' for i in range(num_timesteps)]

            single_series_data = {
                "start": datetime_list[0], # The start of the series
                "target": target_values,
                "feat_dynamic_real": [dynamic_features_data[key] for key in sorted(dynamic_features_data.keys())], # Sort to ensure consistent order if needed
                "feat_static_cat": [0], # Placeholder for building_id index
                "feat_static_real": [1000.0] # Placeholder for building_area
            }
            input_data_for_scoring_list = [single_series_data]
            
        # Wrap the list of series in a "data" key as expected by the score.py run function
        payload_to_ml_endpoint = {"data": input_data_for_scoring_list}
        logger.info(f"Payload to ML Endpoint (sample first series start and target length): start={payload_to_ml_endpoint['data'][0]['start']}, target_len={len(payload_to_ml_endpoint['data'][0]['target'])}")


    except ValueError as e:
        logger.error(f"Invalid request body format: {str(e)}")
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
        # Send the constructed payload_to_ml_endpoint directly as the JSON body
        response = requests.post(endpoint_url, json=payload_to_ml_endpoint, headers=headers, timeout=300)
        response.raise_for_status()

        try:
            predictions = response.json()
            logger.info(f"Successfully received response from endpoint. Keys: {list(predictions.keys())}")
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON response from endpoint: {response.text}")
            return func.HttpResponse(f"Invalid response format from endpoint", status_code=500)

    except requests.exceptions.Timeout:
        logger.error("Endpoint call timed out")
        return func.HttpResponse("Endpoint timeout", status_code=504)
    except requests.exceptions.RequestException as e:
        logger.error(f"Endpoint call failed: {str(e)}")
        return func.HttpResponse(f"Endpoint error: {str(e)}", status_code=500)

    try:
        # The score.py now returns {'predictions': [{...}, {...}]}
        if "predictions" in predictions and isinstance(predictions["predictions"], list):
            prediction_values = predictions["predictions"] # This will be a list of dictionaries, each with 'mean', 'start', etc.
            # For storing in DB, we'll store the entire predictions list as JSONB
            prediction_jsonb = json.dumps(prediction_values)
            logger.info(f"Extracted {len(prediction_values)} series predictions from endpoint response.")
        else:
            logger.warning(f"Unexpected prediction format from endpoint. Expected 'predictions' key with a list. Raw response: {predictions}")
            prediction_jsonb = json.dumps({"raw_response": predictions}) # Store raw response if format unexpected
            prediction_values = [] # Empty list to avoid errors if not a list

        timestamp = datetime.now()

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

        cur.execute(
            "INSERT INTO predictions (timestamp, prediction) VALUES (%s, %s)",
            (timestamp, prediction_jsonb)
        )
        conn.commit()
        cur.close()
        conn.close()

        logger.info(f"Prediction stored in database for {len(prediction_values)} series.")

    except psycopg2.Error as e:
        logger.error(f"Database error: {str(e)}")
        return func.HttpResponse(f"Database error: {str(e)}", status_code=500)
    except Exception as e:
        logger.error(f"Error processing predictions or saving to DB: {str(e)}")
        return func.HttpResponse(f"Prediction processing or DB save error: {str(e)}", status_code=500)

    return func.HttpResponse(
        json.dumps({
            "status": "success", 
            "message": "Predictions stored successfully",
            "number_of_series_predicted": len(prediction_values),
            "sample_first_prediction_mean_length": len(prediction_values[0]['mean']) if prediction_values else 0
        }),
        status_code=200,
        mimetype="application/json"
    )
