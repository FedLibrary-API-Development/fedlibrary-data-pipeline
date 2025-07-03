import requests
import logging
import time
from fedpipeline.config import API_CONFIG, CREDENTIALS

current_token = None

def get_token_cached():
    global current_token
    if not current_token:
        current_token = get_new_token()
    return current_token

def get_new_token():
    payload = {
        "public_v1_user": {
            "email": CREDENTIALS["email"],
            "password": CREDENTIALS["password"]
        }
    }
    
    max_retries = 3
    retry_delay = 2 # seconds
    
    for attempt in range(max_retries):
        try:
            logging.info(f"Getting New Token.... (attempt {attempt + 1}/{max_retries})")
            response = requests.post(API_CONFIG["LOGIN_URL"], json=payload, timeout=(10, 30))
            response.raise_for_status()
            token = response.headers.get("Authorization")
            if not token:
                logging.error("Authorization token not found.")
            else:
                logging.info("Token retrieved successfully")
                return token
        
        except requests.exceptions.Timeout:
            logging.warning(f"Token request timeout (attempt {attempt + 1}/{max_retries})")
        except requests.exceptions.ConnectionError:
            logging.warning(f"Connection error during token request (attempt {attempt + 1}/{max_retries})")
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP error during token request: {e}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Request error during token request: {e}")
        except Exception as e:
            logging.error(f"Unexpected error during token request: {e}")
            
        if attempt < max_retries - 1:
            logging.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay *= 2
            
    logging.error(f"Failed to get new token after {max_retries} attempts")
    return None  

def fetch_data_from_api(url, retry=True):
    global current_token
    try:
        headers = {"Authorization": get_token_cached()}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 401 and retry:
            logging.warning(f"Token expired. Fetching new token and retrying {url}")
            current_token = get_new_token()  # refresh token
            if current_token:
                return fetch_data_from_api(url, retry=False)
            else:
                logging.error("Token refresh failed. Cannot retry.")
                current_token = None
        logging.error(f"HTTP error during fetch from {url}: {e}")
    except Exception as e:
        logging.error(f"Failed to fetch data from {url}: {e}")
    return None