# orchestrator_function/__init__.py
import azure.durable_functions as df
import logging

def orchestrator_function(context: df.DurableOrchestrationContext):
    logging.info("🚀 Orchestrator started")

    try:
        logging.info("🔹 Step 1: Calling CallFetchFirebaseData")
        result1 = yield context.call_activity("CallFetchFirebaseData", None)
        logging.info(f"✅ Step 1 completed: {result1}")

        logging.info("🔹 Step 2: Calling CallTriggerPrediction")
        result2 = yield context.call_activity("CallTriggerPrediction", result1)
        logging.info(f"✅ Step 2 completed: {result2}")

        logging.info("🔹 Step 3: Calling CallOptimizeEnergy")
        result3 = yield context.call_activity("CallOptimizeEnergy", result2)
        logging.info(f"✅ Step 3 completed: {result3}")

        return {
            "fetch_firebase_result": result1,
            "trigger_prediction_result": result2,
            "optimize_energy_result": result3
        }

    except Exception as e:
        logging.error(f"❌ Orchestration failed: {e}", exc_info=True)
        return { "error": str(e) }

main = df.Orchestrator.create(orchestrator_function)
