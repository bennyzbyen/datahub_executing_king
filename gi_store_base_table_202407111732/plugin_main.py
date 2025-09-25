import decimal
import traceback
import sys
import json
import datetime
import clickhouse_connect

import numpy as np
import pandas as pd
from loguru import logger
from plugin_core import *

if __name__ == "__main__":
    params_path = sys.argv[1]
    with open(params_path, 'rb') as f:
        execute_params = json.load(f)
        result = None
        config_path = execute_params.get('config_path')
        algorithm_io_mode = execute_params.get('algorithm_io_mode')
        if algorithm_io_mode == 'SINGLE':
            result = main(execute_params.get('params', {}))
        print(json.dumps(result))
    with open(params_path, 'w') as f:
        f.write(json.dumps(result))
