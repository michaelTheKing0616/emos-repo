# OrchestratorFunction/__init__.py
import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    result1 = yield context.call_activity("FetchFirebaseData", None)
    result2 = yield context.call_activity("TriggerPrediction", result1)
    result3 = yield context.call_activity("OptimizeEnergy", result2)
    return [result1, result2, result3]

main = df.Orchestrator.create(orchestrator_function)
