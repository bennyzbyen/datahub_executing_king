from common_utils.all_modules import logger,json,sys,os
from main_execute import CAL_B5
os.chdir(os.path.dirname(os.path.abspath(__file__)))


def main(params:dict):
    logger.info("---------params---------{}", params)

    supervisor_portal = CAL_B5(params)
    taks_result = supervisor_portal.execute()
    
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
            result = main(params)
        elif algorithm_io_mode is None:
            result_path = './result.txt'
            result = main(execute_params)
    with open(result_path, 'w') as f:
        f.write(json.dumps(result))