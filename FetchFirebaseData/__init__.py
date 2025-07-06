import azure.functions as func
import os
import logging
from azure.storage.blob import BlobServiceClient
from fetch_firebase_data import initialize_firebase, process_snapshot
import psycopg2
from datetime import datetime
from urllib.parse import urlparse, unquote

logger = logging.getLogger("azure")
logger.setLevel(logging.INFO)

def download_credentials():
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['BLOB_CONNECTION_STRING'])
    blob_client = blob_service_client.get_blob_client(container="credentials", blob="firebase_creds.json")
    with open("/tmp/firebase_credentials.json", "wb") as f:
        f.write(blob_client.download_blob().readall())
    return "/tmp/firebase_credentials.json"

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
    conn = psycopg2.connect(**parse_database_url(db_url))
    cur = conn.cursor()

    # Ensure the sensor_data table exists
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
                ts_clean = ts_str.replace("Z", "+00:00")
                ts = datetime.fromisoformat(ts_clean)

                row = (
                    ts,
                    int(building_id),
                    float(values.get("temperature", 0.0)),
                    float(values.get("humidity", 0.0)),
                    int(values.get("occupancy", 0)),
                    float(values.get("energy", 0.0)),
                    float(values.get("current", 0.0)),
                    float(values.get("voltage", 0.0)),
                    float(values.get("power_factor", 1.0))
                )
                rows.append(row)
            except Exception as e:
                logger.warning(f"Invalid row skipped for building {building_id} @ {ts_str}: {e}")

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

    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"Stored {len(rows)} rows of sensor data")

def main(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('Starting FetchFirebase function...')
    try:
        cred_path = download_credentials()
        db_url = os.environ['DATABASE_URL'] or os.environ['TIMESCALEDB_CONNECTION']
        ref = initialize_firebase(cred_path)
        snapshot = ref.get()
        if not snapshot:
            return func.HttpResponse("No sensor data found in Firebase", status_code=404)
        store_sensor_data(snapshot, db_url)
        return func.HttpResponse("Firebase sensor data stored successfully", status_code=200)
    except Exception as e:
        logger.error(f"Error in FetchFirebase function: {e}", exc_info=True)
        return func.HttpResponse(f"Error: {e}", status_code=500)
