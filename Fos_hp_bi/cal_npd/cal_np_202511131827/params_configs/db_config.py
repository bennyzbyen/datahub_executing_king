from common_utils.all_modules import logger, os

env = os.environ.get('running_env') or 'uat'
logger.info(f"running env： {env}")

# environment config
if env == "uat":
    # gateway connection
    app_key = "ef431e71ebca2ebd85f76fa3eb8c6007"
    app_secret = "aa184b3b4d7181715323d52e205bb6eaef431e71ebca2ebd85f76fa3eb8c6007"
    fs_root_dir  = '/datahub/project_storage/execute_king_2025/fos_bi/'
    fs_save_bysku_dir = f"{fs_root_dir}zo_bysku_detail"
    fs_save_calendar_dir = f"{fs_root_dir}mars_calendar"

    clickhouse_connect_params = {'CLICKHOUSE_HOST': '10.216.3.92',
                                 'CLICKHOUSE_PORT': '8123',
                                 'CLICKHOUSE_USER': 'inkstone',
                                 'CLICKHOUSE_PASSWORD': 'U+jeKgAL',
                                 'CLICKHOUSE_DB': 'supervisor_dashboard'}
    clickhouse_token = ''
    cluster = ''

elif env == "prod":
    # gateway connection
    app_key = "2e1ec4de71f806c02e5c746e4a396a4b"
    app_secret = "5371565d62db62fd54189153af5998e52e1ec4de71f806c02e5c746e4a396a4b"
    fs_root_dir  = '/datahub/project_storage/execute_king_2025/fos_bi/'
    fs_save_bysku_dir = f"{fs_root_dir}zo_bysku_detail"
    fs_save_calendar_dir = f"{fs_root_dir}mars_calendar"
    
    # clickhoues connection
    clickhouse_connect_params = {'CLICKHOUSE_HOST': '10.216.3.90',
                                 'CLICKHOUSE_PORT': '8123',
                                 'CLICKHOUSE_USER': 'cl_defa',
                                 'CLICKHOUSE_PASSWORD': 'LC+ziNrP',
                                 'CLICKHOUSE_DB': 'supervisor_dashboard'}
    clickhouse_token = ''
    cluster = 'ON CLUSTER cl_1shards_2replicas'
