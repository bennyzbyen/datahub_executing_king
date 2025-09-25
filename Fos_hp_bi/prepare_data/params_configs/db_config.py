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
    url = "https://bizmp-qa2.mars-ad.net/zuul/bell-dpm"
    process_uid = ['2325','2326']

elif env == "prod":
    # gateway connection
    app_key = "2e1ec4de71f806c02e5c746e4a396a4b"
    app_secret = "5371565d62db62fd54189153af5998e52e1ec4de71f806c02e5c746e4a396a4b"
    fs_root_dir  = '/datahub/project_storage/execute_king_2025/fos_bi/'
    fs_save_bysku_dir = f"{fs_root_dir}zo_bysku_detail"
    fs_save_calendar_dir = f"{fs_root_dir}mars_calendar"
    url = "https://bizmp2.mars-ad.net/zuul/bell-dpm"
    process_uid = ['1889','1890']
    
