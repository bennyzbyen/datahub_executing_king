import os
from loguru import logger

def get_GATEWAY_URL():
    running_env = os.environ.get('running_env') or 'qa'
    logger.info(f"data gateway api running env： {running_env}")
    if running_env == 'qa':
        GATEWAY_URL  = 'https://bizmp-qa2.mars-ad.net/moon'
    elif running_env == 'prod':
        GATEWAY_URL  = 'https://bizmp2.mars-ad.net/moon'
    elif running_env == 'dev':
        GATEWAY_URL = 'http://localhost:17251'
    else:
        GATEWAY_URL  = 'https://bizmp-qa2.mars-ad.net/moon'
    return GATEWAY_URL

NONCE = '0ea2dacc35575ff2'

def AUTH_URL():
    return get_GATEWAY_URL() + '/auth/sign-in'

def HBASE_ASYNC_COUNT_URL():
    return get_GATEWAY_URL() + '/v2/hbase/hbase-table/count'

def HBASE_SYNC_COUNT_URL():
    return get_GATEWAY_URL()  + '/v2/hbase/hbase-table/scan-count'

def HBASE_ASYNC_TASK_STATUS_URL():
    return get_GATEWAY_URL()  + '/v2/hbase/async-task-status'

def HBASE_EXPORT_URL():
    return get_GATEWAY_URL()  + '/v2/hbase/hbase-table/export'

def HBASE_SCAN_URL():
    return get_GATEWAY_URL()  + '/v2/hbase/hbase-table/scan-export'

def HBASE_TRUNCATE_URL():
    return get_GATEWAY_URL()  + '/v2/hbase/hbase-table/truncate'

def HBASE_ASYNC_IMPORT_URL():
    return get_GATEWAY_URL()  + '/v2/hbase/hbase-table/import'

def HBASE_ASYNC_DELETE_URL():
    return get_GATEWAY_URL()  + '/v2/hbase/hbase-table/delete'

def HBASE_SAMPLE_URL():
    return get_GATEWAY_URL()  + '/v2/hbase/hbase-table/sample-data'

def FS_LISTDIR_URL():
    return get_GATEWAY_URL()  + '/v2/files/listdir'

def FS_EXISTS_URL():
    return get_GATEWAY_URL()  + '/v2/files/exists'

def FS_UPLOAD_URL():
    return get_GATEWAY_URL()  + '/v2/files/upload'

def FS_DOWNLOAD_URL():
    return get_GATEWAY_URL()  + '/v2/files/download'

def FS_DELETE_URL():
    return get_GATEWAY_URL()  + '/v2/files/delete'

def FS_RENAME_URL():
    return get_GATEWAY_URL()  + '/v2/files/rename'

def FS_CHUNK_UPLOAD_URL():
    return get_GATEWAY_URL()  + '/v2/files/chunk-upload'

def FS_CHUNK_UPLOAD_STATUS_URL():
    return get_GATEWAY_URL()  + '/v2/files/chunk-upload-status'


def FS_PATH_META_URL():
    return get_GATEWAY_URL()  + '/v2/files/metadata'

