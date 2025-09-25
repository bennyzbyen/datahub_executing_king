from gateway.operate import get_authorization
from hbase.operate_main import count_async, count_sync, query_df_async, query_df_sync, insert_df_async, insert_df_sync, \
    insert_file_async, insert_file_sync, truncate_table, delete_df_sync, get_hbase_table_columns, \
    get_hbase_table_sample, export_table_async
import pandas as pd
import hbase.operate_common as operate_common
from fs.operate import FsClient


class HbaseClient:
    
    __authorization = None

    def __init__(self, app_key, app_secret, fs_root_dir):
        self.__authorization = get_authorization(app_key, app_secret)
        operate_common.fs = FsClient(app_key, app_secret)
        if fs_root_dir:
            operate_common.fs_root_dir = fs_root_dir
        
    def count(self, hbase_table_name, row_start = None, row_stop = None, row_prefixs: [] = None, mode: str = 'reduce'):
        if mode == 'reduce':
            num = count_async(self.__authorization, hbase_table_name, row_start, row_stop, row_prefixs)
        elif mode == 'query':
            num = count_sync(self.__authorization, hbase_table_name, row_start, row_stop, row_prefixs)
        else:
            raise BaseException(f"mode should in ['query', 'reduce']")
        return num
    
    def query_df(self, hbase_table_name: str, columns: [], row_start: str=None, row_stop: str = None, row_prefixs: [] = None, value_filters = {}, mode: str = 'export', **args):
        if mode == 'export':
            return query_df_async(self.__authorization, hbase_table_name, columns, row_start, row_stop, row_prefixs, value_filters, **args)
        elif mode == 'query':
            return query_df_sync(self.__authorization, hbase_table_name, columns, row_start, row_stop, row_prefixs, value_filters, **args)
        else:
            raise BaseException(f"mode should in ['query','export']")

    def async_export_table(self, hbase_table_name: str, columns: [], row_start: str=None, row_stop: str = None, row_prefixs: [] = None, value_filters = {}, mode: str = 'export', **args):
        return export_table_async(self.__authorization, hbase_table_name, columns, row_start, row_stop, row_prefixs, value_filters, **args)
            
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


