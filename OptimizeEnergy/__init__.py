# OptimizeEnergy/__init__.py
import azure.functions as func
from optimize_energy import generate_recommendations
import os
import logging
from pathlib import Path

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Generating recommendations')
    pred_file = Path(os.environ['PREDICTIONS_FILE'])
    sensor_file = Path(os.environ['SENSOR_FILE'])
    db_connection = os.environ['TIMESCALEDB_CONNECTION']
    output_dir = Path(os.environ['OUTPUT_DIR'])
    try:
        generate_recommendations(str(pred_file), str(sensor_file), db_connection, str(output_dir))
        return func.HttpResponse("Recommendations stored", status_code=200)
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(f"Error: {e}", status_code=500)