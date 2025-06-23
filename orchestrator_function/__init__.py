# OrchestratorFunction/__init__.py
import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    yield context.call_activity("FetchFirebaseData", None)
    yield context.call_activity("TriggerPrediction", None)
    yield context.call_activity("OptimizeEnergy", None)
    return "Workflow complete"

main = df.Orchestrator.create(orchestrator_function)
