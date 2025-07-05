import logging
import azure.functions as func
import os
import json
import requests
import psycopg2
from datetime import datetime, timedelta
import numpy as np
from urllib.parse import urlparse, quote

logger = logging.getLogger("azure")
logger.setLevel(logging.INFO)

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
        return func.HttpResponse("Missing environment variables", status_code=500)

    try:
        try:
            req_body = req.get_json()
        except ValueError:
            req_body = None

        if req_body and "data" in req_body:
            original_data = req_body
        else:
            logger.info("Generating default test data")
            num_timesteps = 5  # Reduced for testing
            base_datetime = datetime.utcnow() - timedelta(hours=num_timesteps)
            target_values = [50.5, 51.0, 51.5, 52.0, 52.5]
            dynamic_features_data = [
                [20.0, 20.5, 21.0, 21.5, 22.0],
                [60.0, 60.5, 61.0, 61.5, 62.0],
                [1, 1, 0, 0, 1],
                [50.0, 51.0, 51.5, 52.0, 52.5],
                [5.0, 5.1, 5.2, 5.3, 5.4],
                [230.0, 230.5, 231.0, 231.5, 232.0],
                [0.5, 0.55, 0.6, 0.65, 0.7]
            ]
            original_data = {
                "data": [{
                    "datetime": base_datetime.isoformat(timespec='seconds') + 'Z',
                    "target": target_values,
                    "feat_dynamic_real": dynamic_features_data,
                    "feat_static_cat": [0],
                    "feat_static_real": [1000.0]
                }]
            }

        input_data = { "data": original_data["data"] }

    except Exception as e:
        logger.error(f"Input error: {str(e)}", exc_info=True)
        return func.HttpResponse("Invalid input", status_code=400)

    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

        logger.info(f"Sending request to inference endpoint with data: {json.dumps(input_data, indent=2)}")
        response = requests.post(endpoint_url, headers=headers, data=json.dumps(input_data))
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response text (first 500 chars): {response.text[:500]}")
        response.raise_for_status()

        raw_response = response.text
        predictions = None

        try:
            predictions = json.loads(raw_response)
            if isinstance(predictions, str):
                predictions = json.loads(predictions)
                logger.warning("Double-encoded JSON detected and decoded.")
        except json.JSONDecodeError:
            logger.error("Failed to decode JSON response", exc_info=True)
            return func.HttpResponse("Invalid response format", status_code=500)

        logger.info(f"Parsed keys: {list(predictions.keys()) if isinstance(predictions, dict) else 'Not a dict'}")

        if not isinstance(predictions, dict):
            return func.HttpResponse("Invalid response format", status_code=500)

        prediction_values = predictions.get("forecast", [])
        recommendations_values = predictions.get("recommendations", [])

        if not prediction_values:
            return func.HttpResponse("No predictions returned", status_code=500)

        # Safely parse database URL
        parsed = urlparse(db_url)
        safe_password = quote(parsed.password) if parsed.password else ""
        conn = psycopg2.connect(
            dbname=parsed.path.lstrip("/"),
            user=parsed.username,
            password=safe_password,
            host=parsed.hostname,
            port=parsed.port
        )
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                timestamp TIMESTAMP NOT NULL,
                building_id INT NOT NULL,
                predicted_energy DOUBLE PRECISION,
                anomaly TEXT,
                PRIMARY KEY (timestamp, building_id)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS recommendations (
                timestamp TIMESTAMP NOT NULL,
                building_id INT NOT NULL,
                predicted_energy DOUBLE PRECISION,
                recommendation JSONB,
                PRIMARY KEY (timestamp, building_id)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sensor_data (
                timestamp TIMESTAMP NOT NULL,
                building_id INT NOT NULL,
                temperature DOUBLE PRECISION,
                humidity DOUBLE PRECISION,
                occupancy INT,
                energy DOUBLE PRECISION,
                current DOUBLE PRECISION,
                voltage DOUBLE PRECISION,
                power_factor DOUBLE PRECISION,
                PRIMARY KEY (timestamp, building_id)
            );
        """)

        building_id = original_data["data"][0].get("feat_static_cat", [0])[0]
        start = datetime.fromisoformat(sanitize_iso_timestamp(original_data["data"][0]["datetime"]))
        dynamic_data = original_data["data"][0]["feat_dynamic_real"]
        target = original_data["data"][0]["target"]
        timestamps = [start + timedelta(hours=i) for i in range(len(target))]

        predictions_bulk = []
        recommendations_bulk = []
        sensor_data_bulk = []
        anomaly_tags_bulk = []

        for i, ts in enumerate(timestamps):
            pred = prediction_values[0][i] if i < len(prediction_values[0]) else None
            rec = recommendations_values[i] if i < len(recommendations_values) else {}

            predictions_bulk.append((ts, building_id, pred))
            recommendations_bulk.append((ts, building_id, pred, json.dumps(rec)))

            temp, hum, occ, energy, current, voltage, pf = [f[i] for f in dynamic_data]

            anomaly = None
            if not (10 <= temp <= 40):
                anomaly = f"temperature_out_of_range:{temp}"
            elif not (0.4 <= pf <= 1.0):
                anomaly = f"power_factor_abnormal:{pf}"

            sensor_data_bulk.append((ts, building_id, temp, hum, occ, energy, current, voltage, pf))

            if anomaly:
                anomaly_tags_bulk.append((ts, building_id, anomaly))

        cur.executemany("""
            INSERT INTO predictions (timestamp, building_id, predicted_energy)
            VALUES (%s, %s, %s)
            ON CONFLICT (timestamp, building_id) DO UPDATE
              SET predicted_energy = EXCLUDED.predicted_energy
        """, predictions_bulk)

        cur.executemany("""
            INSERT INTO recommendations (timestamp, building_id, predicted_energy, recommendation)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (timestamp, building_id) DO UPDATE
              SET predicted_energy = EXCLUDED.predicted_energy,
                  recommendation = EXCLUDED.recommendation
        """, recommendations_bulk)

        cur.executemany("""
            INSERT INTO sensor_data (timestamp, building_id, temperature, humidity, occupancy, energy, current, voltage, power_factor)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, building_id) DO UPDATE
              SET temperature = EXCLUDED.temperature,
                  humidity = EXCLUDED.humidity,
                  occupancy = EXCLUDED.occupancy,
                  energy = EXCLUDED.energy,
                  current = EXCLUDED.current,
                  voltage = EXCLUDED.voltage,
                  power_factor = EXCLUDED.power_factor
        """, sensor_data_bulk)

        if anomaly_tags_bulk:
            args_str = ','.join(cur.mogrify("(%s, %s, %s)", x).decode('utf-8') for x in anomaly_tags_bulk)
            cur.execute(f"""
                INSERT INTO predictions (timestamp, building_id, anomaly)
                VALUES {args_str}
                ON CONFLICT (timestamp, building_id) DO UPDATE
                  SET anomaly = EXCLUDED.anomaly
            """)

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"Processing error: {e}", exc_info=True)
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)

    return func.HttpResponse(
        json.dumps({
            "status": "success",
            "stored_predictions": len(predictions_bulk),
            "stored_sensor_data": len(sensor_data_bulk),
            "stored_recommendations": len(recommendations_bulk),
            "anomalies_detected": len(anomaly_tags_bulk)
        }),
        mimetype="application/json"
    )
