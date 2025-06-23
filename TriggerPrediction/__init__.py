import logging
import azure.functions as func
import os
import json
import requests
import psycopg2
from datetime import datetime, timedelta
import numpy as np

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
            input_data = req_body
        else:
            logger.info("Generating default test data")
            num_timesteps = 745
            base_datetime = datetime.utcnow() - timedelta(hours=num_timesteps)
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
        logger.error(f"Input error: {str(e)}", exc_info=True)
        return func.HttpResponse("Invalid input", status_code=400)

    try:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }

        logger.info("Sending request to inference endpoint...")
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

        prediction_values = predictions.get("predictions", [])
        recommendations_values = predictions.get("recommendations", [])

        if not prediction_values:
            return func.HttpResponse("No predictions returned", status_code=500)

        conn = psycopg2.connect(db_url)
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
                recommendation JSONB
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
                power_factor DOUBLE PRECISION
            );
        """)

        def safe_add_pk(cur, table_name, pk_name):
            cur.execute(f"""
                SELECT timestamp, building_id
                FROM public.{table_name}
                GROUP BY timestamp, building_id
                HAVING COUNT(*) > 1
            """)
            duplicates = cur.fetchall()
            if duplicates:
                logger.warning(f"Duplicate entries in {table_name}: {duplicates}")
                return
            try:
                cur.execute(f"""
                    ALTER TABLE public.{table_name}
                    ADD CONSTRAINT {pk_name} PRIMARY KEY (timestamp, building_id)
                """)
                logger.info(f"Primary key added to {table_name}")
            except psycopg2.errors.DuplicateObject:
                logger.info(f"Primary key on {table_name} already exists.")
            except Exception as e:
                logger.error(f"Error adding primary key to {table_name}: {e}", exc_info=True)

        safe_add_pk(cur, "predictions", "predictions_pk")
        safe_add_pk(cur, "recommendations", "recommendations_pk")
        safe_add_pk(cur, "sensor_data", "sensor_data_pk")

        building_id = input_data["data"][0].get("feat_static_cat", [0])[0]
        start = datetime.fromisoformat(sanitize_iso_timestamp(input_data["data"][0]["start"]))
        dynamic_data = input_data["data"][0]["feat_dynamic_real"]
        target = input_data["data"][0]["target"]
        timestamps = [start + timedelta(hours=i) for i in range(len(target))]

        predictions_bulk = []
        recommendations_bulk = []
        sensor_data_bulk = []
        anomaly_tags_bulk = []

        for i, ts in enumerate(timestamps):
            pred = prediction_values[0]["mean"][i] if i < len(prediction_values[0]["mean"]) else None
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
