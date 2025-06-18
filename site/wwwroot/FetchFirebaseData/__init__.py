import azure.functions as func
import os
import logging
from azure.storage.blob import BlobServiceClient
from fetch_firebase_data import initialize_firebase, process_snapshot

def download_credentials():
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['BLOB_CONNECTION_STRING'])
    blob_client = blob_service_client.get_blob_client(container="credentials", blob="firebase_credentials.json")
    with open("/tmp/firebase_credentials.json", "wb") as f:
        f.write(blob_client.download_blob().readall())
    return "/tmp/firebase_credentials.json"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing Firebase data')
    try:
        cred_path = download_credentials()
        db_connection = os.environ['TIMESCALEDB_CONNECTION']
        ref = initialize_firebase(cred_path)
        snapshot = ref.get()
        process_snapshot(snapshot, db_connection)
        return func.HttpResponse("Data fetched and stored", status_code=200)
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"Error: {e}", status_code=500)