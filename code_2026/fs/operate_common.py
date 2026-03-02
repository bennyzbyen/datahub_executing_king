from gateway.conf import FS_LISTDIR_URL, FS_EXISTS_URL, FS_UPLOAD_URL, FS_DOWNLOAD_URL, FS_DELETE_URL, FS_RENAME_URL, FS_CHUNK_UPLOAD_URL, FS_CHUNK_UPLOAD_STATUS_URL
from gateway.operate import get_gateway_headers, call_get_gateway_api, call_post_gateway_api
from io import BytesIO
import io
import os
import warnings
from urllib3.exceptions import InsecureRequestWarning
warnings.filterwarnings("ignore", category=InsecureRequestWarning)
from loguru import logger
import datetime
import hashlib
import random
import pandas as pd
import uuid
import urllib
import time


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


def copy_from_local(authorization, local_path: str, dest_path: str, overwrite=False):
    logger.info(f"local_path: {local_path} to dest_path: {dest_path}")
    if not overwrite:
        is_exists = exists(authorization, dest_path)
        if is_exists:
            raise BaseException(f"file_path {dest_path} exist!")

    file_size = os.path.getsize(local_path)
    if file_size <= 800 * 1024 * 1024:  # 800MB
        logger.info("File size is less than or equal to 800MB.")
        params = {
            "file_path": dest_path
        }
        files = local_path
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

    else:
        logger.info("File size is larger than or equal to 800MB.")
        serial_number = uuid.uuid4()
        chunk_size = 500 * 1024 * 1024  # 500MB
        file_name = os.path.basename(local_path)
        encoded_file_name = urllib.parse.quote(file_name, safe="")

        with open(local_path, "rb") as file:
            chunk_number = 0
            remaining_size = file_size
            while True:
                chunk = file.read(chunk_size)
                remaining_size_mb = remaining_size / (1024 * 1024)
                logger.info(f"remaining_size:{remaining_size_mb:.2f}MB")
                if not chunk:
                    break

                inclusive_range_start = chunk_number * chunk_size
                inclusive_range_end = (chunk_number + 1) * chunk_size - 1
                if inclusive_range_end >= file_size:
                    inclusive_range_end = file_size - 1

                headers = {
                    "Content-Type": "application/octet-stream",
                    "Content-Disposition": f'attachment; filename="{encoded_file_name}"',
                    "Content-Range": f"bytes {inclusive_range_start}-{inclusive_range_end}/{file_size}",
                    "x-moon-authorization": authorization,
                }

                params = {"file_path": dest_path, "uuid": serial_number}

                res = call_post_gateway_api(FS_CHUNK_UPLOAD_URL, data=chunk, headers=headers, params=params, verify=False)

                if res.status_code != 200:
                    raise BaseException(f"上传第 {chunk_number} 块时出错, {res.status_code}, {res.text}")
                else:
                    result = res.json()
                    if result['successful']:
                        status = result["object"]["status"]
                        logger.info(f"上传第 {chunk_number} 块完成, 状态为:{status}")

                        if status == "Failed":
                            raise BaseException(f"api result error: {res.text}")
                    else:
                        raise BaseException(f"上传第 {chunk_number} 块时出错, {res.text}")

                chunk_number += 1
                remaining_size -= chunk_size
                remaining_size = max(0, remaining_size)

        logger.info("所有文件块上传完成，等待服务器执行余下工序")

        while True:
            headers = {"x-moon-authorization": authorization}

            params = {"uuid": serial_number}

            response = call_get_gateway_api(FS_CHUNK_UPLOAD_STATUS_URL, headers=headers, params=params, verify=False)
            if response.status_code != 200:
                print(
                    f"获取 chunk-upload 状态时出错, {response.status_code}, {response.text}"
                )
                break
            else:
                payload = response.json()
                successful = payload["successful"]
                if successful == True:
                    status = payload["object"]["status"]
                    logger.info(f"获取 chunk-upload 状态完成: {status}")

                    if status == "Processing":
                        time.sleep(15)
                    elif status == "Finished":
                        logger.info("文件上传完成")
                        break
                    elif status == "Failed":
                        logger.error("文件上传失败")
                        break
                else:
                    logger.info(f"获取 chunk-upload 状态时出错, {response.text}")
                    break
        
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
    is_file_object = isinstance(data, (io.IOBase, io.TextIOBase))
    if is_file_object:
        new_io.write(data.getvalue())
    else:
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
    logger.info(f"rename: {params}")
    res = call_post_gateway_api(FS_RENAME_URL, headers=headers, params=params, verify=False)
    if res.ok:
        result = res.json()
        if result['successful']:
            logger.info(f"fs operate sucess!!")
        else:
            raise BaseException(f"api result error: {result['err_message']}")
    else:
        raise BaseException(f"-->call gateway api error: {res.text}")
    return True

# def rename(authorization, path: str, destination: str):
#     logger.info(f"path: {path}, destination: {destination}")
#     headers = get_gateway_headers(authorization)
#     if path.endswith("/"):
#         path = path[:-1]
#     if destination.endswith("/"):
#         destination = destination[:-1]
#     try:
#         path_list = listdir(authorization, path)
#         if not exists(authorization, destination):
#             mkdirs(authorization, destination)
#     except:
#         path_list = [""]
#         destination_dir = destination.replace("/" + destination.split("/")[-1], "")
#         if not exists(authorization, destination_dir):
#             mkdirs(authorization, destination_dir)
#     for file in path_list:
#         old_path = f"{path}/{file}"
#         destination_path = f"{destination}/{file}"
#         if old_path.endswith("/"):
#             old_path = old_path[:-1]
#         if destination_path.endswith("/"):
#             destination_path = destination_path[:-1]
#         params = {
#             "old_file_path": old_path,
#             "new_file_path": destination_path
#         }
#         logger.info(f"rename: {params}")
#         res = call_post_gateway_api(FS_RENAME_URL, headers=headers, params=params, verify=False)
#         if res.ok:
#             result = res.json()
#             if result['successful']:
#                 logger.info(f"fs operate sucess!!")
#             else:
#                 raise BaseException(f"api result error: {result['err_message']}")
#         else:
#             raise BaseException(f"-->call gateway api error: {res.text}")
#     return True
        
def mkdirs(authorization, dest_path):
    is_exists = exists(authorization, dest_path)
    if is_exists:
        raise BaseException(f"file_path {dest_path} exist!")
    headers = get_gateway_headers(authorization)
    with open(empty_file, 'wb') as f:
        f.write(b'')
    copy_from_local(authorization, empty_file, dest_path + '/' + empty_file, overwrite=True)
    os.remove(empty_file)
    
    
def read_df(authorization, hdfs_path, format="tsv", **config):
    if config.get('datatype') is not None:
        datatype = config.get('datatype', {})
        config['dtype'] = datatype
        config.pop('datatype', None)
    if exists(authorization, hdfs_path):
        tmp_local_csv_file = f"{str(datetime.datetime.now().strftime('%Y%m%d%H%M%S%f'))}_{generate_uuid()}_temp"
        copy_to_local(authorization, hdfs_path, tmp_local_csv_file)
        if format == 'tsv' or format == 'csv':
            df = pd.read_csv(tmp_local_csv_file, **config)
        elif format == 'parquet':
            columns = config.get('columns', None)
            filters = config.get('filters', None)
            df = pd.read_parquet(tmp_local_csv_file, columns=columns, filters=filters)
        else:
            df = None
        os.remove(tmp_local_csv_file)
        return df
    else:
        raise BaseException(f"hdfs {hdfs_path} not exists")
