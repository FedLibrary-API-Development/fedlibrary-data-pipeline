import requests
import logging
from fedpipeline.config import API_CONFIG, CREDENTIALS

def get_token():
    payload = {
        "email": CREDENTIALS["email"],
        "password": CREDENTIALS["password"]
    }
    try:
        response = requests.post(API_CONFIG["LOGIN_URL"], json=payload)
        response.raise_for_status()
        token = response.headers.get("Authorization")
        if not token:
            logging.error("Authorization token not found.")
        return token
    except Exception as e:
        logging.error(f"Failed to login and fetch token: {e}")
        return None

def fetch_data_from_api(url, token):
    try:
        headers = {"Authorization": token}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json().get("items", [])
    except Exception as e:
        logging.error(f"Failed to fetch data from {url}: {e}")
        return []
