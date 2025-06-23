import azure.functions as func
import azure.durable_functions as df
import logging

async def main(mytimer: func.TimerRequest, starter: str):
    client = df.DurableOrchestrationClient(starter)
    instance_id = await client.start_new("orchestrator_function", None)
    logging.info(f"Started orchestration with ID = '{instance_id}'")
