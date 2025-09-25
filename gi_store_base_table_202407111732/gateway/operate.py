import requests
import hashlib
from gateway.conf import AUTH_URL, NONCE
import warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.filterwarnings("ignore", category=InsecureRequestWarning)



import time
from loguru import logger

def call_retry(max_attempts=5, delay=10):
        def decorator(func):
            def wrapper(*args, **kwargs):
                attempts = 0
                while attempts < max_attempts:
                    try:
                        return func(*args, **kwargs)
                    except Exception as e:
                        logger.error(e)
                        attempts += 1
                        logger.info(f"retry run num: {max_attempts - attempts + 1}")
                        time.sleep(delay)
                print(f"{func.__name__} not success call !!")
            return wrapper
        return decorator

@call_retry(max_attempts=5, delay=30)
def call_get_gateway_api(url, headers, params, stream=False, verify=False):
    return requests.get(url, headers=headers, params=params, stream=stream, verify=verify, timeout=3*60)

@call_retry(max_attempts=5, delay=30)
def call_post_gateway_api(url, headers, params=None, json=None, files=None, verify=False):
    return requests.post(url, headers=headers, params=params, json=json, files=files, verify=verify, timeout=3*60)

def get_gateway_token(app_key, app_secret):
    sha256 = hashlib.sha256()
    sha256.update((app_key + app_secret + NONCE).encode('utf-8'))
    signature = sha256.hexdigest()
    headers = {
        'Content-Type': 'application/json'
    }
    body = {
        'app_key': app_key,
        'nonce': NONCE,
        'signature': signature
    }
    res = call_post_gateway_api(url=AUTH_URL, headers=headers, json=body, verify=False)
    res = res.json()
    if res.get('successful'):
        access_token = res.get('object').get('access_token')
    else:
        raise Exception(f"get access_token fail: {res.get('err_message')}")
    return access_token

def get_authorization(app_key, app_secret):
    return 'bearer ' +  get_gateway_token(app_key, app_secret) 

def get_gateway_headers(authorization, content_type = 'application/json'):
    return {
        'x-moon-authorization': authorization
    }
