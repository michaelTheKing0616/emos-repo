import logging
import azure.functions as func
import os
import json
import requests
import psycopg2
from psycopg2 import extras
from urllib.parse import urlparse, unquote
from datetime import datetime, timedelta

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

def parse_database_url(db_url):
    parsed = urlparse(db_url)
    return {
        "dbname": parsed.path.lstrip("/"),
        "user": parsed.username,
        "password": unquote(parsed.password),
        "host": parsed.hostname,
        "port": parsed.port,
        "sslmode": "require"
    }

def main(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("Function TriggerPrediction started")

    endpoint_url = os.getenv("ENDPOINT_URL")
    api_key = os.getenv("API_KEY")
    db_url = os.getenv("DATABASE_URL")

    if not all([endpoint_url, api_key, db_url]):
        return func.HttpResponse("Missing environment variables", status_code=500)

    try:
        req_body = req.get_json()
    except ValueError:
        req_body = None

    try:
        if req_body and "data" in req_body:
            original_data = req_body
        else:
            logger.info("Generating default test data")
            num_timesteps = 5
            base_datetime = datetime.utcnow() - timedelta(hours=num_timesteps)
            original_data = {
                "data": [{
                    "datetime": base_datetime.isoformat(timespec='seconds') + 'Z',
                    "target": [50 + i * 0.5 for i in range(num_timesteps)],
                    "feat_dynamic_real": [
                        [20 + i * 0.5 for i in range(num_timesteps)],
                        [60 + i * 0.5 for i in range(num_timesteps)],
                        [1, 1, 0, 0, 1],
                        [50 + i for i in range(num_timesteps)],
                        [5 + i * 0.1 for i in range(num_timesteps)],
                        [230 + i * 0.5 for i in range(num_timesteps)],
                        [0.5 + i * 0.05 for i in range(num_timesteps)]
                    ],
                    "feat_static_cat": [0],
                    "feat_static_real": [1000.0],
                    "item_id": "meter_001"
                }]
            }

        logger.info(f"Sending request to inference endpoint with data: {json.dumps(original_data, indent=2)}")

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

        response = requests.post(endpoint_url, headers=headers, data=json.dumps(original_data))
        logger.info(f"Response status: {response.status_code}")
        logger.info(f"Response text (first 500 chars): {response.text[:500]}")
        response.raise_for_status()

        predictions = json.loads(response.text)
        if isinstance(predictions, str):
            predictions = json.loads(predictions)

        forecast = predictions.get("forecast", [])
        recommendations = predictions.get("recommendations", [])
        anomalies = predictions.get("anomalies", [])
        postgres_ready = predictions.get("postgres_ready", [])  # <-- New field

        if not forecast:
            return func.HttpResponse("No forecast returned", status_code=500)

        conn = psycopg2.connect(**parse_database_url(db_url))
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

        record = original_data["data"][0]
        building_id = record.get("feat_static_cat", [0])[0]
        start = datetime.fromisoformat(sanitize_iso_timestamp(record["datetime"]))
        dynamic_data = record["feat_dynamic_real"]
        target = record["target"]
        timestamps = [start + timedelta(hours=i) for i in range(len(target))]

        predictions_bulk = []
        recommendations_bulk = []
        sensor_data_bulk = []

        for i, ts in enumerate(timestamps):
            pred = forecast[0][i] if isinstance(forecast[0], list) else forecast[i]
            temp, hum, occ, energy, current, voltage, pf = [f[i] for f in dynamic_data]

            anomaly = None
            if not (10 <= temp <= 40):
                anomaly = f"temperature_out_of_range:{temp}"
            elif not (0.4 <= pf <= 1.0):
                anomaly = f"power_factor_abnormal:{pf}"

            predictions_bulk.append((ts, building_id, pred, anomaly))
            sensor_data_bulk.append((ts, building_id, temp, hum, occ, energy, current, voltage, pf))

        # Use postgres_ready if available
        if postgres_ready:
            recommendations_bulk = [
                (
                    datetime.fromisoformat(sanitize_iso_timestamp(rec["timestamp"])),
                    int(rec["building_id"]),
                    float(rec["predicted_energy"]),
                    json.dumps(rec["recommendation"])
                )
                for rec in postgres_ready
            ]
        else:
            for i, ts in enumerate(timestamps):
                pred = forecast[0][i] if isinstance(forecast[0], list) else forecast[i]
                rec = recommendations[i] if i < len(recommendations) else {}
                recommendations_bulk.append((ts, building_id, pred, json.dumps(rec)))

        cur.executemany("""
            INSERT INTO predictions (timestamp, building_id, predicted_energy, anomaly)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (timestamp, building_id) DO UPDATE
              SET predicted_energy = EXCLUDED.predicted_energy,
                  anomaly = EXCLUDED.anomaly
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
            "anomalies_detected": sum(1 for _, _, _, a in predictions_bulk if a)
        }),
        mimetype="application/json"
    )
