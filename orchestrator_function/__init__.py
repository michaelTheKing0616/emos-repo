# orchestrator_function/__init__.py
import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    log = context.create_replay_safe_logger()
    log.info("🚀 Orchestrator started")

    try:
        log.info("🔹 Step 1: Calling CallFetchFirebaseData")
        result1 = yield context.call_activity("CallFetchFirebaseData", None)
        log.info(f"✅ Step 1 completed: {result1}")

        log.info("🔹 Step 2: Calling CallTriggerPrediction")
        result2 = yield context.call_activity("CallTriggerPrediction", result1)
        log.info(f"✅ Step 2 completed: {result2}")

        log.info("🔹 Step 3: Calling CallOptimizeEnergy")
        result3 = yield context.call_activity("CallOptimizeEnergy", result2)
        log.info(f"✅ Step 3 completed: {result3}")

        return {
            "fetch_firebase_result": result1,
            "trigger_prediction_result": result2,
            "optimize_energy_result": result3
        }

    except Exception as e:
        log.error(f"❌ Orchestration failed: {e}", exc_info=True)
        return { "error": str(e) }

main = df.Orchestrator.create(orchestrator_function)
