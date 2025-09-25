import os
from loguru import logger

#running_env = os.environ.get('running_env') or 'uat'
running_env = 'prod'
logger.info(f"running env： {running_env}")
if running_env == 'uat':
    GATEWAY_URL = 'https://bizmp-qa2.mars-ad.net/moon'
elif running_env == 'prod':
    GATEWAY_URL = 'https://bizmp2.mars-ad.net/moon'
else:
    GATEWAY_URL = 'https://bizmp-qa2.mars-ad.net/moon'
AUTH_URL = GATEWAY_URL + '/auth/sign-in'
HBASE_ASYNC_COUNT_URL = GATEWAY_URL + '/v2/hbase/hbase-table/count'
HBASE_SYNC_COUNT_URL = GATEWAY_URL + '/v2/hbase/hbase-table/scan-count'
HBASE_ASYNC_TASK_STATUS_URL = GATEWAY_URL + '/v2/hbase/async-task-status'
HBASE_EXPORT_URL = GATEWAY_URL + '/v2/hbase/hbase-table/export'
HBASE_TRUNCATE_URL = GATEWAY_URL + '/v2/hbase/hbase-table/truncate'
HBASE_ASYNC_IMPORT_URL = GATEWAY_URL + '/v2/hbase/hbase-table/import'
HBASE_ASYNC_DELETE_URL = GATEWAY_URL + '/v2/hbase/hbase-table/delete'
HBASE_SAMPLE_URL = GATEWAY_URL + '/v2/hbase/hbase-table/sample-data'
FS_LISTDIR_URL = GATEWAY_URL + '/v2/files/listdir'
FS_EXISTS_URL = GATEWAY_URL + '/v2/files/exists'
FS_UPLOAD_URL = GATEWAY_URL + '/v2/files/upload'
FS_DOWNLOAD_URL = GATEWAY_URL + '/v2/files/download'
FS_DELETE_URL =  GATEWAY_URL + '/v2/files/delete'
FS_RENAME_URL = GATEWAY_URL + '/v2/files/rename'
NONCE = '0ea2dacc35575ff2'