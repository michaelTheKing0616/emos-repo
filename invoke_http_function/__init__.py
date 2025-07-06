# invoke_http_function/__init__.py
import azure.functions as func
import requests
import logging
import json

def main(params: dict) -> str:
    function_name = params.get("function_name", "UnknownFunction")
    url = params.get("url")

    if not url:
        logging.error(f"{function_name}: No URL provided in parameters.")
        return f"Failed: {function_name} - Missing URL"

    try:
        logging.info(f"Invoking {function_name} via POST {url}")
        response = requests.post(url)

        logging.info(f"{function_name} response code: {response.status_code}")
        if response.status_code >= 400:
            logging.error(f"{function_name} error: {response.text}")
            return f"Failed: {function_name} - HTTP {response.status_code}"

        try:
            content = response.json()
        except Exception:
            content = response.text

        logging.info(f"{function_name} succeeded. Response content: {json.dumps(content) if isinstance(content, dict) else content}")
        return f"Success: {function_name}"

    except Exception as e:
        logging.exception(f"Exception while invoking {function_name}: {str(e)}")
        return f"Exception: {str(e)}"
