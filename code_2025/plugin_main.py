import sys
import json
from execute_king import EXECUTE_KING
from all_configs.database_config import logger



def calc_single(params:dict):
    logger.info("---------params---------{}", params)
    execute_king = EXECUTE_KING(params)
    taks_result = execute_king.execute()

    
    return taks_result

if __name__ == "__main__":
    params_path = sys.argv[1]
    with open(params_path, 'rb') as f:
        execute_params = json.load(f)
        result = None
        config_path = execute_params.get('config_path')
        algorithm_io_mode = execute_params.get('algorithm_io_mode',None)
        result_path = params_path
        if algorithm_io_mode == 'SINGLE':
            execute_params = execute_params.get('params', {})
            params = json.loads(execute_params['body']['params'])
            result = calc_single(params)
        elif algorithm_io_mode is None:
            result_path = './result.txt'
            result = calc_single(execute_params)
    with open(result_path, 'w') as f:
        f.write(json.dumps(result))