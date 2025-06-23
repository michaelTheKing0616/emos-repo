import azure.durable_functions as df

def orchestrator_function(context: df.DurableOrchestrationContext):
    yield context.call_activity("invoke_http_function", {
        "function_name": "FetchFirebaseData",
        "url": "emos-functions-v2.azurewebsites.net/api/FetchFirebaseData?code=oPTgxtKHq21QxYM_gFpj5WL6R_qFocP9CfiOEdLtqMnJAzFua_pVeg=="
    })
    
    yield context.call_activity("invoke_http_function", {
        "function_name": "TriggerPrediction",
        "url": "emos-functions-v2.azurewebsites.net/api/TriggerPrediction?code=Je2-IZzsO2kj3lUhIfj_rovGMRsh_E6N9J3p51khORX6AzFu5U9VZg=="
    })

    # Optional: Add this if OptimizeEnergy is ready
    # yield context.call_activity("invoke_http_function", {
    #     "function_name": "OptimizeEnergy",
    #     "url": "<YOUR_FUNCTION_BASE_URL>/api/OptimizeEnergy"
    # })

    return "Workflow completed."

main = df.Orchestrator.create(orchestrator_function)
