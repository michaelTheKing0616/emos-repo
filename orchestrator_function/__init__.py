# orchestrator_function/__init__.py
import azure.durable_functions as df
import logging

def orchestrator_function(context: df.DurableOrchestrationContext):
    logging.info("Starting orchestrator_function")

    try:
        # Step 1: Fetch sensor data from Firebase and store in TimescaleDB
        result1 = yield context.call_activity("FetchFirebaseData", None)
        logging.info(f"FetchFirebaseData completed: {result1}")

        # Step 2: Run prediction using updated model and save results to TimescaleDB
        result2 = yield context.call_activity("TriggerPrediction", result1)
        logging.info(f"TriggerPrediction completed: {result2}")

        # Step 3: Generate post-prediction optimization recommendations
        result3 = yield context.call_activity("OptimizeEnergy", result2)
        logging.info(f"OptimizeEnergy completed: {result3}")

        return {
            "fetch_firebase_result": result1,
            "trigger_prediction_result": result2,
            "optimize_energy_result": result3
        }

    except Exception as e:
        logging.error(f"Orchestration failed: {e}", exc_info=True)
        return {
            "error": str(e)
        }

main = df.Orchestrator.create(orchestrator_function)
