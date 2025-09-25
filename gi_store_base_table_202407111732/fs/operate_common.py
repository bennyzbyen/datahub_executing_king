from gateway.conf import FS_LISTDIR_URL, FS_EXISTS_URL, FS_UPLOAD_URL, FS_DOWNLOAD_URL, FS_DELETE_URL, FS_RENAME_URL
from gateway.operate import get_gateway_headers, call_get_gateway_api, call_post_gateway_api
from io import BytesIO
import os
import warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.filterwarnings("ignore", category=InsecureRequestWarning)
from loguru import logger
import datetime
import hashlib
import random
def generate_uuid():
    data = str(random.getrandbits(256)).encode('utf-8')  # 生成随机的256位长的二进制数据
    hash_value = hashlib.md5(data).hexdigest()  # 对数据进行MD5哈希
    return hash_value

empty_file = '.__init__'


def listdir(authorization, dest_path):
    logger.info(f"list dest_path: {dest_path}")
    headers = get_gateway_headers(authorization)
    params = {
        "directory_path": dest_path
    }
    res = call_get_gateway_api(FS_LISTDIR_URL, headers=headers, params=params, verify=False)
    if res.ok:
        result = res.json()
        if result['successful']:
            list_dir = result['object']
            if not list_dir:
                raise BaseException(f"dir {dest_path} does not exist")
            list_result = []
            for file in list_dir:
                path = file['path']
                if not path.startswith("."):
                    list_result.append(path)
            logger.info(f"fs operate sucess!!")
            return list_result
        else:
            raise BaseException(f"api result error: {result['err_message']}")
    else:
        raise BaseException(f"-->call gateway api error: {res.text}")
        
def exists(authorization, dest_path):
    logger.info(f"dest_path: {dest_path}")
    headers = get_gateway_headers(authorization)
    params = {
        "path": dest_path
    }
    res = call_get_gateway_api(FS_EXISTS_URL, headers=headers, params=params, verify=False)
    if res.ok:
        result = res.json()
        if result['successful']:
            logger.info(f"fs operate sucess!!")
            return result['object']
        else:
            raise BaseException(f"api result error: {result['err_message']}")
    else:
        raise BaseException(f"-->call gateway api error: {res.text}")
        
def copy_from_local(authorization, local_path: str, dest_path: str,  overwrite=False):
    logger.info(f"local_path: {local_path} to dest_path: {dest_path}")
    if not overwrite:
        is_exists = exists(authorization, dest_path)
        if is_exists:
            raise BaseException(f"file_path {dest_path} exist!")
    params = {
        "file_path": dest_path
    }
    files = {
        'filename': local_path,
        'Content-Disposition': 'form-data;',
        'file': (local_path,open(local_path,'rb'))
    }
    headers = get_gateway_headers(authorization)
    res = call_post_gateway_api(FS_UPLOAD_URL, headers=headers, params=params, files=files, verify=False)
    if res.ok:
        result = res.json()
        if result['successful']:
            logger.info(f"fs operate sucess!!")
            return result['object']
        else:
            raise BaseException(f"api result error: {result['err_message']}")
    else:
        raise BaseException(f"-->call gateway api error: {res.text}")
        
def copy_to_local(authorization,dest_path: str, local_path: str):
    logger.info(f"dest_path: {dest_path} to local_path: {local_path}")
    headers = get_gateway_headers(authorization)
    params = {
        "file_path": dest_path
    }
    res = call_get_gateway_api(FS_DOWNLOAD_URL, headers=headers, params=params, stream=True, verify=False)
    if res.ok:
        chunk_size = 1024 * 1024 * 100
        with open(local_path, 'wb') as f:
            for chunk in res.iter_content(chunk_size = chunk_size):
                if chunk:
                    f.write(chunk)
        logger.info(f"fs operate sucess!!")
    else:
        raise BaseException(f"-->call gateway api error: {res.text}")
        
def open_fs(authorization,dest_path: str):
    logger.info(f"dest_path: {dest_path}")
    is_exists = exists(authorization, dest_path)
    if not is_exists:
        raise BaseException(f"file_path {dest_path} not exist!")
    headers = get_gateway_headers(authorization)
    params = {
        "file_path": dest_path
    }
    res = call_get_gateway_api(FS_DOWNLOAD_URL, headers=headers, params=params, stream=True, verify=False)
    if res.ok:
        logger.info(f"fs operate sucess!!")
        return BytesIO(res.content)   
    else:
        raise BaseException(f"-->call gateway api error: {res.text}")
        
def append(authorization,dest_path,data):
    logger.info(f"dest_path: {dest_path}")
    file_io = open_fs(authorization, dest_path)
    new_io = BytesIO()
    new_io.write(file_io.getvalue())
    #print(data.encode('utf-8'))
    new_io.write(data.encode('utf-8'))
    local_tmp_file = f"./_temp_{str(datetime.datetime.now().strftime('%Y%m%d%H%M%S%f'))}_{generate_uuid()}"
    with open(local_tmp_file, 'wb') as f:
        f.write(new_io.getvalue())
    copy_from_local(authorization, local_tmp_file, dest_path, overwrite=True)
    logger.info(f"fs operate sucess!!")
    os.remove(local_tmp_file)

    
def delete(authorization,dest_path, recursive = False):
    logger.info(f"dest_path: {dest_path}")
    headers = get_gateway_headers(authorization)
    params = {
        "path": dest_path,
        "recursive": recursive
    }
    res = call_post_gateway_api(FS_DELETE_URL, headers=headers, params=params, verify=False)
    if res.ok:
        result = res.json()
        if result['successful']:
            logger.info(f"fs operate sucess!!")
            return result['object']
        else:
            raise BaseException(f"api result error: {result['err_message']}")
    else:
        raise BaseException(f"-->call gateway api error: {res.text}")

def rename(authorization, path: str, destination: str):
    logger.info(f"path: {path}, destination: {destination}")
    headers = get_gateway_headers(authorization)
    params = {
        "old_file_path": path,
        "new_file_path": destination
    }
    res = call_post_gateway_api(FS_RENAME_URL, headers=headers, params=params, verify=False)
    if res.ok:
        result = res.json()
        if result['successful']:
            logger.info(f"fs operate sucess!!")
            return result['object']
        else:
            raise BaseException(f"api result error: {result['err_message']}")
    else:
        raise BaseException(f"-->call gateway api error: {res.text}")
        
def mkdirs(authorization, dest_path):
    is_exists = exists(authorization, dest_path)
    if is_exists:
        raise BaseException(f"file_path {dest_path} exist!")
    headers = get_gateway_headers(authorization)
    with open(empty_file, 'wb') as f:
        f.write(b'')
    copy_from_local(authorization, empty_file, dest_path + '/' + empty_file, overwrite=True)
    os.remove(empty_file)
