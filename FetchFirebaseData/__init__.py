import azure.functions as func
import os
import logging
import json
from azure.storage.blob import BlobServiceClient
from fetch_firebase_data import initialize_firebase, process_snapshot
import psycopg2
from datetime import datetime

def download_credentials():
    try:
        logging.info("Attempting to download firebase_credentials.json from Azure Blob...")

        blob_service_client = BlobServiceClient.from_connection_string(os.environ['BLOB_CONNECTION_STRING'])
        blob_client = blob_service_client.get_blob_client(container="credentials", blob="firebase_credentials.json")

        download_path = "/tmp/firebase_credentials.json"
        with open(download_path, "wb") as f:
            blob_data = blob_client.download_blob().readall()
            f.write(blob_data)

        logging.info("Firebase credentials successfully downloaded.")
        return download_path
    except Exception as e:
        logging.error(f"Failed to download Firebase credentials: {e}", exc_info=True)
        raise

def store_sensor_data(snapshot, db_url):
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()

        # Ensure sensor_data table exists
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

        rows = []
        for building_id, readings in snapshot.items():
            for ts_str, values in readings.items():
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    rows.append((
                        ts,
                        int(building_id),
                        float(values.get("temperature", 0)),
                        float(values.get("humidity", 0)),
                        int(values.get("occupancy", 0)),
                        float(values.get("energy", 0)),
                        float(values.get("current", 0)),
                        float(values.get("voltage", 0)),
                        float(values.get("power_factor", 1))
                    ))
                except Exception as e:
                    logging.warning(f"Invalid sensor row (building_id={building_id}, timestamp={ts_str}): {e}")

        if rows:
            cur.executemany("""
                INSERT INTO sensor_data (
                    timestamp, building_id, temperature, humidity, occupancy,
                    energy, current, voltage, power_factor
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp, building_id) DO UPDATE
                SET temperature = EXCLUDED.temperature,
                    humidity = EXCLUDED.humidity,
                    occupancy = EXCLUDED.occupancy,
                    energy = EXCLUDED.energy,
                    current = EXCLUDED.current,
                    voltage = EXCLUDED.voltage,
                    power_factor = EXCLUDED.power_factor
            """, rows)
            logging.info(f"{len(rows)} sensor rows inserted/updated.")
        else:
            logging.info("No valid sensor data rows found in snapshot.")

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        logging.error(f"Database error: {e}", exc_info=True)
        raise

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("FetchFirebaseData function triggered.")

    try:
        cred_path = download_credentials()

        db_url = os.environ['TIMESCALEDB_CONNECTION']
        if not db_url:
            raise ValueError("Missing TIMESCALEDB_CONNECTION in environment variables")

        ref = initialize_firebase(cred_path)
        logging.info("Firebase connection established.")

        snapshot = ref.get()
        if not snapshot:
            logging.warning("No data found in Firebase.")
            return func.HttpResponse("No sensor data found in Firebase.", status_code=204)

        logging.info("Sensor data snapshot fetched.")
        store_sensor_data(snapshot, db_url)

        return func.HttpResponse("Firebase sensor data stored successfully.", status_code=200)

    except Exception as e:
        logging.error(f"FetchFirebaseData failed: {e}", exc_info=True)
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
