from gateway.conf import *
from gateway.operate import get_gateway_headers, call_get_gateway_api, call_post_gateway_api
import time
import datetime
import pandas as pd
import os
from loguru import logger
import warnings
import hashlib
import random
from urllib3.exceptions import InsecureRequestWarning
warnings.filterwarnings("ignore", category=InsecureRequestWarning)



fs = None

fs_root_dir = None

export_hdfs_tmp_dir = "export"

import_hdfs_tmp_dir = "import"

delete_hdfs_tmp_dir = "delete"

family_name = "info"

default_insert_sep = "\x1D"

#default_insert_sep = "\t"

default_export_sep = "\t"

default_insert_row_key = 'HBASE_ROW_KEY'


default_row_prefixs = ['0','1','2','3','4','5','6','7', '8','9','']

args_record_file_name = '.args_record.txt'


def generate_uuid():
    data = str(random.getrandbits(256)).encode('utf-8')  # 生成随机的256位长的二进制数据
    hash_value = hashlib.md5(data).hexdigest()  # 对数据进行MD5哈希
    return hash_value


def get_async_task_result(authorization, tid):
    while (True):
        headers = get_gateway_headers(authorization)
        task_body = {
            "tid": tid
        }
        status_res = call_get_gateway_api(HBASE_ASYNC_TASK_STATUS_URL, headers=headers, params=task_body, verify=False)
        status_res = status_res.json()      
        if status_res['successful']:
            status_code = status_res['object']['async_task_status_code']
            if status_code == 1:
                # 延迟调用
                time.sleep(15)
                logger.info(f"the async job is running ...")
            elif status_code == 2:
                if status_res['object'].get('result', None) is not None:
                    return status_res['object']['result']
                else:
                    return True
            else:
                raise BaseException(f"-->call py hbase error: response: {status_res}")
        else:
            raise BaseException(f"-->call py hbase error: response: {status_res}")
        
def get_count_async(authorization, hbase_table_name, row_start = None, row_stop = None, row_prefixs: [] = default_row_prefixs):
    logger.info(f"start count: {hbase_table_name}")
    body = {
        'hbase_table_name': hbase_table_name,
        'row_stop': row_start,
        'row_stop': row_stop,
        'row_prefixs': row_prefixs
    }
    headers = get_gateway_headers(authorization)
    res = call_post_gateway_api(url=HBASE_ASYNC_COUNT_URL, headers=headers, json=body, verify=False)
    if res.ok:
        result = res.json()
        if result['successful']:
            count = int(get_async_task_result(authorization, result.get('object').get('tid')).get('number_of_rows'))
            logger.info(f"end count: {hbase_table_name}")
            return count
        else:
            raise BaseException(f"api result error: {result['err_message']}")
    else:
        raise BaseException(f"api result error: {res.text}")

def get_count_sync(authorization,  hbase_table_name, row_start = None, row_stop = None, row_prefixs: [] = default_row_prefixs):
    logger.info(f"start count: {hbase_table_name}")
    body = {
        'hbase_table_name': hbase_table_name,
        'row_stop': row_start,
        'row_stop': row_stop,
        'row_prefixs': row_prefixs
    }
    headers = get_gateway_headers(authorization)
    res = call_post_gateway_api(url=HBASE_SYNC_COUNT_URL, headers=headers, json=body, verify=False)
    logger.info(f"end count: {hbase_table_name}")
    if res.ok:
        result = res.json()
        if result['successful']:
            count = int(res.json().get('object').get('number_of_rows'))
            logger.info(f"end count: {hbase_table_name}")
            return count
        else:
            raise BaseException(f"api result error: {result['err_message']}")
    else:
        raise BaseException(f"api result error: {res.text}")


def get_delete_hdfs_for_hbase(df: pd.DataFrame, hbase_table_name: str, sep: str, **args):
    spec_hdfs_out_dir = args.get('hdfs_out_dir', None)
    columns = df.columns
    if columns[0] != "HBASE_ROW_KEY":
        raise BaseException(f"data first column should be 'HBASE_ROW_KEY'!!")
    local_tsv_file = f"./tmp_data_{str(datetime.datetime.now().strftime('%Y%m%d%H%M%S%f'))}_{generate_uuid()}"
    df = df.rename(columns={'HBASE_ROW_KEY': 'rowkey'})
    df['rowkey'].to_csv(local_tsv_file, index=False, header=False, encoding='utf-8', sep=sep)
    time_now = datetime.datetime.now()
    if spec_hdfs_out_dir is None:
        hdfs_out_dir = f"{fs_root_dir}/{delete_hdfs_tmp_dir}/{time_now.strftime('%Y%m%d')}/{time_now.strftime('%Y%m%d%H%M%S%f')}_{hbase_table_name}"
    else:
        hdfs_out_dir = spec_hdfs_out_dir
    hdfs_file_name = f"{hbase_table_name}.tsv"
    fs.copy_from_local(local_tsv_file, f"{hdfs_out_dir}/{hdfs_file_name}", overwrite=True)
    os.remove(local_tsv_file)
    return hdfs_out_dir, hdfs_file_name
    
    
def hbase_delete_by_rowkey_file(authorization, hbase_table_name, fs_dir, fs_name):
    body = {
        "hbase_table_name": hbase_table_name,
        "input_file_path": f"{fs_dir}/{fs_name}"
    }
    headers = get_gateway_headers(authorization)
    saveArgsRecord(f"{fs_dir}/{args_record_file_name}", fs_dir, body, authorization)
    res = call_post_gateway_api(url=HBASE_ASYNC_DELETE_URL, headers=headers, json=body, verify=False)
    if res.ok:
        result = res.json()
        if result['successful']:
            get_async_task_result(authorization, result.get('object').get('tid'))
            logger.info(f"{hbase_table_name}: end delete...")
        else:
            raise BaseException(f"api result error: {result['err_message']}")
    else:
        raise BaseException(f"api result error: {res.text}") 
        

def insert_async(authorization: str, hbase_table_name: str, hbase_columns:[], sep: str, hdfs_out_dir, hdfs_file_name):
    logger.info(f"{hbase_table_name}: start insert...")
    request_body = {
        "hbase_table_name": f"{hbase_table_name}",
        "columns": ",".join(hbase_columns),
        "input_file_path": f"{hdfs_out_dir}/{hdfs_file_name}",
        "separator": sep
    }
    headers = get_gateway_headers(authorization)
    saveArgsRecord(f"{hdfs_out_dir}/{args_record_file_name}", hdfs_out_dir, request_body, authorization)
    res = call_post_gateway_api(url=HBASE_ASYNC_IMPORT_URL, headers=headers, json=request_body, verify=False)
    if res.ok:
        result = res.json()
        if result['successful']:
            get_async_task_result(authorization, result.get('object').get('tid'))
            logger.info(f"{hbase_table_name}: end insert...")
        else:
            raise BaseException(f"api result error: {result['err_message']}")
    else:
        raise BaseException(f"api result error: {res.text}")    

    
    
def get_insert_hdfs_for_hbase(df: pd.DataFrame, hbase_table_name: str, sep: str, **args):
    spec_hdfs_out_dir = args.get('hdfs_out_dir', None)
    special_columns = args.get('special_columns', [])
    compression = args.get('compression', True)
    if compression:
        suffix = '.gz'
        compression_type = 'gzip'
    else:
        suffix = ''
        compression_type = None
    for key in special_columns:
        logger.info(f"开始处理字段{key}")
        df[key] = df[key].fillna('')
        df.loc[((df[key].isin(['None', 'nan'])) | (df[key].isnull())), key] = ''
        df[key] = df[key].apply(lambda x: x.replace('\t', '').replace('\n', '').replace('\r', ''))
    columns = df.columns
    if columns[0] != "HBASE_ROW_KEY":
        raise BaseException(f"data first column should be 'HBASE_ROW_KEY'!!")
    local_tsv_file = f"./tmp_data_{datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')}_{generate_uuid()}{suffix}"
    df.to_csv(local_tsv_file, index=False, encoding='utf-8', sep=sep, header=False, compression=compression_type)
    time_now = datetime.datetime.now()
    if spec_hdfs_out_dir is None:
        hdfs_out_dir = f"{fs_root_dir}/{import_hdfs_tmp_dir}/{time_now.strftime('%Y%m%d')}/{time_now.strftime('%Y%m%d%H%M%S%f')}_{hbase_table_name}"
    else:
        hdfs_out_dir = spec_hdfs_out_dir
    hdfs_file_name = f"{hbase_table_name}.tsv{suffix}"
    fs.copy_from_local(local_tsv_file, f"{hdfs_out_dir}/{hdfs_file_name}", overwrite=True)
    os.remove(local_tsv_file)
    return hdfs_out_dir, hdfs_file_name


def upload_insert_hdfs_for_hbase(file_path: str, hbase_table_name: str, **args):
    spec_hdfs_out_dir = args.get('hdfs_out_dir', None)
    time_now = datetime.datetime.now()
    if spec_hdfs_out_dir is None:
        hdfs_out_dir = f"{fs_root_dir}/{import_hdfs_tmp_dir}/{time_now.strftime('%Y%m%d')}/{time_now.strftime('%Y%m%d%H%M%S%f')}_{hbase_table_name}"
    else:
        hdfs_out_dir = spec_hdfs_out_dir
    if ".gz" in file_path:
        hdfs_file_name = f"{hbase_table_name}.tsv.gz"
    else:
        hdfs_file_name = f"{hbase_table_name}.tsv"
    fs.copy_from_local(file_path, f"{hdfs_out_dir}/{hdfs_file_name}", overwrite=True)
    return hdfs_out_dir, hdfs_file_name

    

def get_df_by_hdfs_dir(hdfs_dir_path: str, columns: [], export_format):
    files = fs.listdir(hdfs_dir_path)
    files = list(filter(lambda x: not x.startswith("_SUCCESS") and x != args_record_file_name, files))
    result_df = None
    for file in files:
        file_path = f"{hdfs_dir_path}/{file}"
        df = readHDFS(fs, file_path, sep=default_export_sep, datatype=str, export_format=export_format)
        df = df[['rowkey'] + columns]
        result_df = pd.concat([result_df, df], axis=0)
        df = None
    return result_df

def create_directory(target_path: str, base_path: str = 'export_files'):
    timestamp = str(int(time.time()))
    new_dir_name = f"{base_path}{os.sep}{target_path}_{timestamp}"
    os.makedirs(new_dir_name)
    logger.info(f"Directory already exists. Created new directory: {new_dir_name}")
    return new_dir_name

def copy_file_from_hdfs(hbase_table_name: str, hdfs_dir_path: str, columns: [], export_format) -> list:
    dir_name = create_directory(hbase_table_name)
    files = fs.listdir(hdfs_dir_path)
    files = list(filter(lambda x: not x.startswith("_SUCCESS") and x != args_record_file_name, files))
    local_files = []
    for file in files:
        file_path = f"{hdfs_dir_path}/{file}"
        local_path = copyHDFS(dir_name, fs, file_path, sep=default_export_sep, datatype=str, export_format=export_format)
        local_files.append(local_path)
    return local_files

def hbase_export_async(authorization, hbase_table_name, family_name, columns, **extParams):
    row_start = extParams.get("row_start", None)
    row_stop = extParams.get("row_stop", None)
    row_prefixs = extParams.get("row_prefixs", default_row_prefixs)
    export_format = extParams.get("export_format")
    if row_prefixs is None:
        row_prefixs = default_row_prefixs
    value_filters = extParams.get("value_filters",{})
    time_now = datetime.datetime.now()
    hdfs_out_dir = f"{fs_root_dir}/{export_hdfs_tmp_dir}/{time_now.strftime('%Y%m%d')}/{time_now.strftime('%Y%m%d%H%M%S%f')}_{hbase_table_name}"
    request_body = {
        "hbase_table_name": f"{hbase_table_name}",
        "column_qualifiers": ",".join(columns),
        "output_directory_path": hdfs_out_dir,
        "column_family": family_name,
        "row_key_start": row_start,
        "row_key_stop": row_stop,
        "row_key_prefixes": row_prefixs,
        "value_filters": value_filters,
        "export_format": export_format
    }
    saveArgsRecord(f"{hdfs_out_dir}/{args_record_file_name}", hdfs_out_dir, request_body, authorization)
    headers = get_gateway_headers(authorization)
    res = call_post_gateway_api(url=HBASE_EXPORT_URL, headers=headers, json=request_body, verify=False)
    if res.ok:
        result = res.json()
        if result['successful']:
            get_async_task_result(authorization, result.get('object').get('tid'))
            return hdfs_out_dir
        else:
            raise BaseException(f"api result error: {result['err_message']}")
    else:
        raise BaseException(f"api result error: {res.text}")


def saveArgsRecord(hdfs_args_path, hdfs_record_dir, args, authorization):
    if fs.exists(f"{hdfs_args_path}") == False:
        local_path = f"./args_init_{str(datetime.datetime.now().strftime('%Y%m%d%H%M%S%f'))}"
        with open(local_path, 'w', encoding='utf8') as f:
            f.write(f"hdfs_record_dir,args,authorization\n{hdfs_record_dir},{args},{authorization}\n")
        fs.copy_from_local(local_path, f"{hdfs_args_path}", overwrite=True)
        os.remove(local_path)
    else:
        fs.append(f"{hdfs_args_path}",f"{hdfs_record_dir},{args},{authorization}\n")


def readHDFS(fs, hdfs_path, **config):
    sep = config.get('sep', default_export_sep)
    datatype = config.get('datatype', {})
    header = config.get('header', 0)
    columns = config.get('columns', None)
    export_format = config.get('export_format', 'tsv')
    if fs.exists(hdfs_path):
        tmp_local_csv_file = f"{str(datetime.datetime.now().strftime('%Y%m%d%H%M%S%f'))}_{generate_uuid()}_temp"
        fs.copy_to_local(hdfs_path, tmp_local_csv_file)
        if export_format == 'tsv':
            if header is None:
                df = pd.read_csv(tmp_local_csv_file, sep=sep, dtype=datatype, header=None, names=columns)
            else:
                df = pd.read_csv(tmp_local_csv_file, sep=sep, dtype=datatype)
        elif export_format == 'parquet':
                df = pd.read_parquet(tmp_local_csv_file, columns=columns)
        os.remove(tmp_local_csv_file)
        return df
    else:
        raise BaseException(f"hdfs {hdfs_path} not exists")

def copyHDFS(local_dir, fs, hdfs_path, **config):
    sep = config.get('sep', default_export_sep)
    datatype = config.get('datatype', {})
    header = config.get('header', 0)
    columns = config.get('columns', None)
    export_format = config.get('export_format', 'tsv')
    if fs.exists(hdfs_path):
        tmp_local_csv_file = local_dir + os.sep + hdfs_path.split('/')[-1] + f'.{export_format}'
        fs.copy_to_local(hdfs_path, tmp_local_csv_file)
        return tmp_local_csv_file
        # if export_format == 'tsv':
        #     if header is None:
        #         df = pd.read_csv(tmp_local_csv_file, sep=sep, dtype=datatype, header=None, names=columns)
        #     else:
        #         df = pd.read_csv(tmp_local_csv_file, sep=sep, dtype=datatype)
        # elif export_format == 'parquet':
        #         df = pd.read_parquet(tmp_local_csv_file, columns=columns)
        # os.remove(tmp_local_csv_file)
        # return df
    else:
        raise BaseException(f"hdfs {hdfs_path} not exists")

def truncate(authorization, hbase_table_name):
    logger.info(f"start truncate {hbase_table_name}")
    headers = get_gateway_headers(authorization)
    body = {
        "hbase_table_name": hbase_table_name
    }
    res = call_post_gateway_api(HBASE_TRUNCATE_URL, headers=headers, json=body, verify=False)
    if res.ok:
        result = res.json()
        if result['successful']:
            get_async_task_result(authorization, result.get('object').get('tid'))
            logger.info(f"end truncate {hbase_table_name}")
            return True
        else:
            raise BaseException(f"api result error: {result['err_message']}")
    else:
        raise BaseException(f"-->call gateway api error: {res.text}")


def get_table_info(authorization, hbase_table_name):
    body = {
        'hbase_table_name': hbase_table_name
    }
    headers = get_gateway_headers(authorization)
    res = call_get_gateway_api(url=HBASE_SAMPLE_URL, headers=headers, params=body, verify=False)
    if res.ok:
        result = res.json()
        if result['successful']:
            return result['object']
        else:
            raise BaseException(f"api result error: {result['err_message']}")
    else:
        raise BaseException(f"-->call gateway api error: {res.text}")


def get_table_columns(authorization, hbase_table_name):
    table_info = get_table_info(authorization, hbase_table_name)
    columns = [name.replace(family_name + ":", "") for name in table_info['head']]
    return columns


def get_table_sample(authorization, hbase_table_name):
    table_info = get_table_info(authorization, hbase_table_name)
    columns = [name for name in table_info['head']]
    return [{name.replace(family_name + ":", ""): map.get(name, '') for name in columns} for map in table_info['body']]
