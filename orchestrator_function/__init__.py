# orchestrator_function/__init__.py
import azure.durable_functions as df
import logging

def orchestrator_function(context: df.DurableOrchestrationContext):
    log = context.create_replay_safe_logger(logging.getLogger("durable.orchestrator"))
    log.info("🚀 Starting orchestrator_function")

    try:
        # Step 1: Call FetchFirebaseData
        log.info("🔁 Step 1: CallFetchFirebaseData")
        result1 = yield context.call_activity("CallFetchFirebaseData", None)
        log.info(f"✅ CallFetchFirebaseData completed: {result1}")

        # Step 2: Call TriggerPrediction using result1
        log.info("🔁 Step 2: CallTriggerPrediction")
        result2 = yield context.call_activity("CallTriggerPrediction", result1)
        log.info(f"✅ CallTriggerPrediction completed: {result2}")

        # Step 3: Call OptimizeEnergy using result2
        log.info("🔁 Step 3: CallOptimizeEnergy")
        result3 = yield context.call_activity("CallOptimizeEnergy", result2)
        log.info(f"✅ CallOptimizeEnergy completed: {result3}")

        return {
            "fetch_firebase_result": result1,
            "trigger_prediction_result": result2,
            "optimize_energy_result": result3
        }

    except Exception as e:
        log.error(f"❌ Orchestration failed: {e}", exc_info=True)
        return {
            "error": str(e)
        }

main = df.Orchestrator.create(orchestrator_function)
