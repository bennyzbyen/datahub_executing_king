from common_utils.all_modules import requests, time, logger
from params_configs.db_config import url, process_uid

def call_retry(max_attempts=5, delay=10):
	def decorator(func):
		def wrapper(*args, **kwargs):
			attempts = 0
			error = None
			while attempts < max_attempts + 1:
				try:
					return func(*args, **kwargs)
				except Exception as e:
					logger.error(e)
					attempts += 1
					logger.info(f"retry run num: {max_attempts - attempts + 1}")
					time.sleep(delay)
			logger.error(f"{func.__name__} not success call !!")
			raise Exception(error)
		return wrapper
	return decorator

class PIPELINE_ACTIVATE:
    def __init__(self):
        pass

    @call_retry(max_attempts=5, delay=10)
    def run_pipeline_process(self, base_url: str, api_path: str, username: str, api_key: str, process_uid: int):
        """
        水线调度
        """
        headers = {
            "Api-Key": api_key,
            "X-Username": username,
            "Content-Type": "application/json"
        }
        params = {
            'process_uid': process_uid
        }
        full_url = f"{base_url}{api_path}"

        try:
            res = requests.request("post", url=full_url, params=params, headers=headers, verify=False)
            res.raise_for_status() 

            result = res.json()

            if result.get('successful'):
                return result
            else:
                error_message = result.get('err_message', 'Unknown API error')
                raise BaseException(f"API result error: {error_message}")

        except requests.exceptions.RequestException as e:
            raise requests.exceptions.RequestException(f"API call failed: {e}") from e
        except Exception as e:
            raise BaseException(f"An unexpected error occurred: {e}") from e

    
    def run(self):
        logger.info(f"Start {self.__class__.__name__}")
        ## 调度水线信息
        running_api = "/build/projects/deliverables/processes/run"
        user_name = "Execute_King_2025_op"
        api_key = "CpVpo8ZR1Ypl1SmPL1evklbyzc3UDjvn"

        for uid in process_uid:
            self.run_pipeline_process(url, running_api, user_name, api_key, uid)
        

        logger.info(f"Finished {self.__class__.__name__}")  