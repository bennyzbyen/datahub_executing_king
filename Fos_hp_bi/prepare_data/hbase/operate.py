from gateway.operate import get_authorization
from hbase.operate_main import count_async, count_sync, export, query_df_async, query_df_sync, get_async_task_result, import_df_async, import_file_async, insert_df_async, insert_df_sync, insert_file_async, insert_gateway_file_async, insert_file_sync, truncate_table, delete_df_sync, get_hbase_table_columns, get_hbase_table_sample
import pandas as pd
import hbase.operate_common as operate_common
from fs.operate import FsClient


class HbaseClient:
    
    __authorization = None

    def __init__(self, app_key, app_secret, fs_root_dir:str):
        self.__authorization = get_authorization(app_key, app_secret)
        operate_common.fs = FsClient(app_key, app_secret)
        if fs_root_dir:
            if fs_root_dir.endswith("/"):
                operate_common.fs_root_dir = fs_root_dir[:-1]
            else:
                operate_common.fs_root_dir = fs_root_dir
        
    def count(self, hbase_table_name, row_start = None, row_stop = None, row_prefixs: [] = None, mode: str = 'reduce'):
        if mode == 'reduce':
            num = count_async(self.__authorization, hbase_table_name, row_start, row_stop, row_prefixs)
        elif mode == 'query':
            num = count_sync(self.__authorization, hbase_table_name, row_start, row_stop, row_prefixs)
        else:
            raise BaseException(f"mode should in ['query', 'reduce']")
        return num

    def export_gateway_fs(self, hbase_table_name,columns= None, row_start = None, row_stop = None, row_prefixs: [] = None, hdfs_out_dir = None, export_file_format= "tsv", **args):
        hdfs_out_dir = export(self.__authorization,hbase_table_name=hbase_table_name, columns=columns, row_start=row_start, row_stop=row_stop, row_prefixs=row_prefixs, hdfs_out_dir=hdfs_out_dir, export_file_format=export_file_format, **args)
        return hdfs_out_dir
    
    def query_df(self, hbase_table_name: str, columns: [], row_start: str=None, row_stop: str = None, row_prefixs: [] = None, value_filters = {}, mode: str = 'export', **args):
        if mode == 'export':
            return query_df_async(self.__authorization, hbase_table_name, columns, row_start, row_stop, row_prefixs, value_filters, **args)
        elif mode == 'query':
            return query_df_sync(self.__authorization, hbase_table_name, columns, row_start, row_stop, row_prefixs, value_filters, **args)
        else:
            raise BaseException(f"mode should in ['query','export']")

    # 异步导入， 返回异步任务tid
    def import_df(self, df: pd.DataFrame, hbase_table_name: str, **args):
        return import_df_async(self.__authorization, df, hbase_table_name, **args)
    
    # 异步导入文件， 返回异步任务tid
    def import_file(self, file_path: str, sep: str, columns: [] = [],  hbase_table_name: str = None):
        return import_file_async(self.__authorization, file_path, sep, columns,  hbase_table_name)

    # 获取异步任务
    def confirm_async_task(self, tid):
        return get_async_task_result(self.__authorization, tid)
            
    def insert_df(self, df: pd.DataFrame, hbase_table_name: str, mode: str = 'import', **args):
        if mode == 'import':
            return insert_df_async(self.__authorization, df,  hbase_table_name, **args)
        elif mode == 'insert':
            return insert_df_sync(self.__authorization, df, hbase_table_name, **args)
        else:
            raise BaseException(f"mode should in ['insert', 'import']")


    def insert_file(self, file_path: str, sep: str, columns: [] = [],  hbase_table_name: str = None, mode: str = 'import'):
        if mode == 'import':
            return insert_file_async(self.__authorization,file_path, sep, columns, hbase_table_name)
        elif mode == 'insert':
            return insert_file_sync(self.__authorization,file_path, sep, columns, hbase_table_name)
        else:
            raise BaseException(f"mode should in ['insert', 'import']")
        
    def insert_fs_file(self, fs_file_path: str, sep: str, columns: [] = [],  hbase_table_name: str = None, mode: str = 'import'):
        return insert_gateway_file_async(self.__authorization,fs_file_path, sep, columns, hbase_table_name)

    def insert_gateway_fs(self, gateway_file_path: str, sep: str, columns: [] = [],  hbase_table_name: str = None, mode: str = 'import'):
        return insert_gateway_file_async(self.__authorization,gateway_file_path, sep, columns, hbase_table_name)

    def truncate(self, hbase_table_name: str):
        truncate_table(self.__authorization, hbase_table_name)


    def delete_df(self, df: pd.DataFrame, hbase_table_name: str):
        delete_df_sync(self.__authorization, df, hbase_table_name)


    def delete(self, hbase_table_name: str, query_by_columns, row_start, row_stop):
        # 先查询导出
        # 删除
        pass

    def table_columns(self, hbase_table_name: str):
        return get_hbase_table_columns(self.__authorization, hbase_table_name)

    def table_sample(self, hbase_table_name: str):
        return get_hbase_table_sample(self.__authorization, hbase_table_name)
    
    def clean_temp_floder(self, days:int=90):
        operate_common.clean_temp_floder(days)


