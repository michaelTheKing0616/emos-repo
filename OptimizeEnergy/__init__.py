# OptimizeEnergy/__init__.py
import azure.functions as func
import logging
import os
from optimize_energy import generate_recommendations_from_db

def main(context: func.Context) -> str:
    logging.info("OptimizeEnergy function triggered by orchestrator")
    try:
        db_connection = os.environ['TIMESCALEDB_CONNECTION']
        generate_recommendations_from_db(db_connection)
        return "Energy recommendations generated successfully."
    except Exception as e:
        logging.error(f"Optimization error: {e}")
        return f"Error during optimization: {e}"
