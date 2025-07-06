# starter_function/__init__.py
import azure.functions as func
import azure.durable_functions as df
import logging
from datetime import datetime

async def main(mytimer: func.TimerRequest, starter: str):
    utc_timestamp = datetime.utcnow().isoformat()
    logging.info(f"starter_function triggered at {utc_timestamp}")

    try:
        client = df.DurableOrchestrationClient(starter)
        instance_id = await client.start_new("orchestrator_function", None)

        logging.info(f"Started orchestration with ID = '{instance_id}'")
    except Exception as e:
        logging.error(f"Failed to start orchestration: {str(e)}", exc_info=True)
