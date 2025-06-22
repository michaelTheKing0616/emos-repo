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

SENSOR_VALIDATION_RANGES = {
    "temperature": (-50, 60),
    "humidity": (0, 100),
    "occupancy": (0, 1000),
    "energy": (0, 100000),
    "current": (0, 1000),
    "frequency": (45, 65),
    "power": (0, 100000),
    "power_factor": (0, 1),
    "voltage": (0, 500)
}

def sanitize_iso_timestamp(ts: str) -> str:
    if ts.endswith('+00:00Z'):
        return ts.replace('+00:00Z', '+00:00')
    elif ts.endswith('Z') and '+00:00' in ts:
        return ts.replace('Z', '')
    elif ts.endswith('Z'):
        return ts.replace('Z', '+00:00')
    return ts

def validate_sensor(name, value):
    if value is None:
        return None
    try:
        lo, hi = SENSOR_VALIDATION_RANGES.get(name, (float("-inf"), float("inf")))
        return value if lo <= value <= hi else None
    except Exception:
        return None

def main(req: func.HttpRequest) -> func.HttpResponse:
    logger.info("Function TriggerPrediction started")

    endpoint_url = os.getenv("ENDPOINT_URL")
    api_key = os.getenv("API_KEY")
    db_url = os.getenv("DATABASE_URL")

    if not all([endpoint_url, api_key, db_url]):
        return func.HttpResponse("Missing configuration", status_code=500)

    try:
        req_body = req.get_json()
    except ValueError:
        req_body = None

    if req_body and "data" in req_body:
        input_data = req_body
    else:
        num_timesteps = 745
        base_datetime = datetime.now() - timedelta(hours=num_timesteps)
        target_values = [50 + np.sin(i/24*np.pi)*10 + np.random.rand()*5 for i in range(num_timesteps)]
        dynamic_features_data = [[np.random.rand()*100 for _ in range(num_timesteps)] for _ in range(9)]
        input_data = {
            "data": [{
                "start": base_datetime.isoformat(timespec='seconds') + 'Z',
                "target": target_values,
                "feat_dynamic_real": dynamic_features_data,
                "feat_static_cat": [0],
                "feat_static_real": [1000.0]
            }]
        }

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
            predictions_json = json.loads(raw_response)
        except json.JSONDecodeError:
            predictions_json = json.loads(json.loads(raw_response))

        building_id = req.params.get("building_id") or "1"
        start_time = datetime.fromisoformat(sanitize_iso_timestamp(input_data["data"][0]["start"]))
        dynamic_feats = input_data["data"][0].get("feat_dynamic_real", [])

        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        predictions_bulk = []
        recommendations_bulk = []
        sensor_data_bulk = []

        for idx, mean_value in enumerate(predictions_json["predictions"][0]["mean"]):
            ts = start_time + timedelta(hours=idx)
            ts_iso = ts.replace(tzinfo=None)

            predictions_bulk.append((ts_iso, building_id, mean_value))

            recommendations_bulk.append((ts_iso, building_id, mean_value, json.dumps({"tag": "ok"})))

            sensor_data_bulk.append((
                ts_iso, building_id,
                validate_sensor("temperature", dynamic_feats[0][idx] if len(dynamic_feats) > 0 else None),
                validate_sensor("humidity", dynamic_feats[1][idx] if len(dynamic_feats) > 1 else None),
                validate_sensor("occupancy", dynamic_feats[2][idx] if len(dynamic_feats) > 2 else None),
                validate_sensor("energy", dynamic_feats[3][idx] if len(dynamic_feats) > 3 else None),
                validate_sensor("current", dynamic_feats[4][idx] if len(dynamic_feats) > 4 else None),
                validate_sensor("frequency", dynamic_feats[5][idx] if len(dynamic_feats) > 5 else None),
                validate_sensor("power", dynamic_feats[6][idx] if len(dynamic_feats) > 6 else None),
                validate_sensor("power_factor", dynamic_feats[7][idx] if len(dynamic_feats) > 7 else None),
                validate_sensor("voltage", dynamic_feats[8][idx] if len(dynamic_feats) > 8 else None)
            )

        cur.executemany("""
            INSERT INTO predictions (timestamp, building_id, predicted_energy)
            VALUES (%s, %s, %s)
            ON CONFLICT (timestamp, building_id) DO UPDATE SET
                predicted_energy = EXCLUDED.predicted_energy
        """, predictions_bulk)

        cur.executemany("""
            INSERT INTO recommendations (timestamp, building_id, predicted_energy, recommendations)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (timestamp, building_id) DO UPDATE SET
                predicted_energy = EXCLUDED.predicted_energy,
                recommendations = EXCLUDED.recommendations
        """, recommendations_bulk)

        cur.executemany("""
            INSERT INTO sensor_data (
                timestamp, building_id, temperature, humidity, occupancy, energy,
                current, frequency, power, power_factor, voltage
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp, building_id) DO UPDATE SET
                temperature = EXCLUDED.temperature,
                humidity = EXCLUDED.humidity,
                occupancy = EXCLUDED.occupancy,
                energy = EXCLUDED.energy,
                current = EXCLUDED.current,
                frequency = EXCLUDED.frequency,
                power = EXCLUDED.power,
                power_factor = EXCLUDED.power_factor,
                voltage = EXCLUDED.voltage
        """, sensor_data_bulk)

        conn.commit()
        cur.close()
        conn.close()

        return func.HttpResponse(
            json.dumps({"status": "success", "prediction_count": len(predictions_bulk)}),
            mimetype="application/json")

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return func.HttpResponse("Internal server error", status_code=500)
