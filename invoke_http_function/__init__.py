import azure.functions as func
import requests
import logging

def main(params: dict) -> str:
    try:
        url = params.get("url")
        function_name = params.get("function_name", "UnknownFunction")

        response = requests.post(url)
        logging.info(f"{function_name} responded with {response.status_code}")

        if response.status_code >= 400:
            logging.error(f"{function_name} failed: {response.text}")
            return f"Failed: {function_name}"
        return f"Success: {function_name}"

    except Exception as e:
        logging.exception(f"Exception calling {params.get('function_name')}: {str(e)}")
        return f"Exception: {str(e)}"
