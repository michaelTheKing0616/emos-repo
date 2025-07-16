import azure.functions as func
import os
import logging
import json
from azure.storage.blob import BlobServiceClient
import psycopg2
from urllib.parse import urlparse, unquote
from datetime import datetime

# Firebase Admin SDK
import firebase_admin
from firebase_admin import credentials, db

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

def initialize_firebase(credential_path: str):
    try:
        logging.info("Initializing Firebase Admin SDK...")

        if not firebase_admin._apps:
            cred = credentials.Certificate(credential_path)
            firebase_admin.initialize_app(cred, {
                "databaseURL": os.environ.get("FIREBASE_DB_URL")
            })
            logging.info("Firebase Admin SDK initialized.")
        else:
            logging.info("Firebase Admin SDK already initialized.")

        return db.reference('/sensor_data')

    except Exception as e:
        logging.error(f"Firebase initialization error: {e}", exc_info=True)
        raise

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

def store_sensor_data(snapshot, db_url):
    try:
        conn = psycopg2.connect(**parse_database_url(db_url))
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
                power DOUBLE PRECISION,
                PRIMARY KEY (timestamp, building_id)
            );
        """)

        rows = []
        flattened = {
            "temperature": [],
            "humidity": [],
            "occupancy": [],
            "energy": [],
            "current": [],
            "voltage": [],
            "power_factor": [],
            "power": []
        }

        # Accept both list- and dict-based Firebase formats
        if isinstance(snapshot, dict):
            iterable = snapshot.items()
        elif isinstance(snapshot, list):
            iterable = enumerate(snapshot)
        else:
            logging.warning("Unsupported Firebase snapshot type.")
            return flattened

        for building_id, readings in iterable:
            if not isinstance(readings, dict):
                continue
            for ts_str, values in readings.items():
                try:
                    ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    temp = float(values.get("temperature", 0))
                    hum = float(values.get("humidity", 0))
                    occ = int(values.get("occupancy", 0))
                    energy = float(values.get("energy", 0))
                    current = float(values.get("current", 0))
                    voltage = float(values.get("voltage", 0))
                    pf = float(values.get("power_factor", 1))
                    power = float(values.get("power", 0))

                    rows.append((ts, int(building_id), temp, hum, occ, energy, current, voltage, pf, power))

                    # Populate flattened
                    flattened["temperature"].append(temp)
                    flattened["humidity"].append(hum)
                    flattened["occupancy"].append(occ)
                    flattened["energy"].append(energy)
                    flattened["current"].append(current)
                    flattened["voltage"].append(voltage)
                    flattened["power_factor"].append(pf)
                    flattened["power"].append(power)

                except Exception as e:
                    logging.warning(f"Invalid row (building_id={building_id}, timestamp={ts_str}): {e}")

        if rows:
            cur.executemany("""
                INSERT INTO sensor_data (
                    timestamp, building_id, temperature, humidity, occupancy,
                    energy, current, voltage, power_factor, power
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (timestamp, building_id) DO UPDATE
                SET temperature = EXCLUDED.temperature,
                    humidity = EXCLUDED.humidity,
                    occupancy = EXCLUDED.occupancy,
                    energy = EXCLUDED.energy,
                    current = EXCLUDED.current,
                    voltage = EXCLUDED.voltage,
                    power_factor = EXCLUDED.power_factor,
                    power = EXCLUDED.power
            """, rows)
            logging.info(f"{len(rows)} sensor rows inserted/updated.")
        else:
            logging.info("No valid sensor data rows found in snapshot.")

        conn.commit()
        cur.close()
        conn.close()

        return flattened

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
        flattened = store_sensor_data(snapshot, db_url)

        return func.HttpResponse(
            json.dumps(flattened),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"FetchFirebaseData failed: {e}", exc_info=True)
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
