from hbase.operate_common import *
import pandas as pd



def count_async(authorization: str, hbase_table_name: str, row_start = None, row_stop = None, row_prefixs: [] = None):
    return get_count_async(authorization, hbase_table_name, row_start, row_stop, row_prefixs)

def count_sync(authorization: str, hbase_table_name: str, row_start = None, row_stop = None, row_prefixs: [] = None):
    return get_count_sync(authorization, hbase_table_name, row_start, row_stop, row_prefixs)


def query_df_sync(authorization: str, hbase_table_name: str, columns: [], row_start: str=None, row_stop: str = None, row_prefixs: [] = None, value_filters = {}, **args):
    filter_columns = [name for name in value_filters.keys()]
    if set(filter_columns).issubset(columns) == False:
        raise BaseException(f"{filter_columns} key not in columns：{columns}")
    # 不指定索引同步查询
    logger.info(f"start export: {hbase_table_name}")
    time_now = datetime.datetime.now()
    hdfs_out_dir = f"{export_hdfs_tmp_dir}/{time_now.strftime('%Y%m%d')}/{time_now.strftime('%Y%m%d%H%M%S%f')}_{hbase_table_name}"
    row_list = args.get("row_list", [])
    hbase_export_sync(authorization, hbase_table_name, family_name, columns, hdfs_out_dir, row_start=row_start, row_stop= row_stop, row_prefixs = row_prefixs, value_filters = value_filters, row_list = row_list)
    logger.info(f"end export: {hbase_table_name}")
    return get_df_by_hdfs_dir(hdfs_out_dir, columns)


def query_df_async(authorization: str, hbase_table_name: str, columns: [], row_start: str=None, row_stop: str = None, row_prefixs: [] = None, value_filters = {}, export_format="tsv"):
    filter_columns = [name for name in value_filters.keys()]
    if set(filter_columns).issubset(columns) == False:
        raise BaseException(f"{filter_columns} key not in columns：{columns}")
    # 不指定索引异步查询
    logger.info(f"start export: {hbase_table_name}")
    hdfs_out_dir = hbase_export_async(authorization, hbase_table_name, family_name, columns,  row_start=row_start, row_stop= row_stop, row_prefixs = row_prefixs, value_filters = value_filters, export_format=export_format)
    logger.info(f"end export: {hbase_table_name}")
    return get_df_by_hdfs_dir(hdfs_out_dir, columns, export_format)

def export_table_async(authorization: str, hbase_table_name: str, columns: [], row_start: str=None, row_stop: str = None, row_prefixs: [] = None, value_filters = {}, export_format="tsv"):
    filter_columns = [name for name in value_filters.keys()]
    if set(filter_columns).issubset(columns) == False:
        raise BaseException(f"{filter_columns} key not in columns：{columns}")
    # 不指定索引异步查询
    logger.info(f"start export: {hbase_table_name}")
    hdfs_out_dir = hbase_export_async(authorization, hbase_table_name, family_name, columns,  row_start=row_start, row_stop= row_stop, row_prefixs = row_prefixs, value_filters = value_filters, export_format=export_format)
    logger.info(f"end export: {hbase_table_name}")
    if row_start is None and row_stop is None:
        return copy_file_from_hdfs(hbase_table_name, hdfs_out_dir, columns, export_format)
    else:
        if row_start is None:
            row_start = ''
        if row_stop is None:
            row_stop = ''
        return copy_file_from_hdfs(f"{hbase_table_name}_{row_start}_{row_stop}", hdfs_out_dir, columns, export_format)


def insert_df_sync(authorization: str, df: pd.DataFrame,  hbase_table_name: str, sep: str = default_insert_sep, **args):
    pass


def insert_df_async(authorization: str, df: pd.DataFrame, hbase_table_name: str, sep: str = default_insert_sep, **args):
    # 1 生成hdfs文件
    hdfs_out_dir, hdfs_file_name = get_insert_hdfs_for_hbase(df, hbase_table_name, sep, **args)


    hbase_columns = [family_name + ":" + name if name != 'HBASE_ROW_KEY' else name for name in df.columns]
    #2 导入hbase
    insert_async(authorization, hbase_table_name, hbase_columns, sep, hdfs_out_dir, hdfs_file_name)



def insert_file_sync(authorization: str, file_path: str, sep: str, columns: [] = [],  hbase_table_name: str = None):
    pass



def insert_file_async(authorization: str, file_path: str,  sep: str, columns: [] = [],  hbase_table_name: str = None):
    hdfs_out_dir, hdfs_file_name =  upload_insert_hdfs_for_hbase(file_path, hbase_table_name)

    hbase_columns = [family_name + ":" + name if name != 'HBASE_ROW_KEY' else name for name in columns]
    #2 导入hbase
    insert_async(authorization, hbase_table_name, hbase_columns, sep, hdfs_out_dir, hdfs_file_name)

def truncate_table(authorization: str, hbase_table_name: str):
    truncate(authorization, hbase_table_name)


def delete_df_sync(authorization: str, df: pd.DataFrame, hbase_table_name: str):
    # 1. 先删除index
    # 生成rowkey hdfs删除文件
    hdfs_out_dir, hdfs_file_name = get_delete_hdfs_for_hbase(df, hbase_table_name, default_export_sep)
    # 5.删除hbase数据
    logger.info(f"start delete hbase_table_name: {hbase_table_name}")
    hbase_delete_by_rowkey_file(authorization, hbase_table_name, f"{hdfs_out_dir}", f"{hdfs_file_name}")
    logger.info(f"end delete hbase_table_name: {hbase_table_name}")


def get_hbase_table_sample(authorization: str,  hbase_table_name: str):
    return get_table_sample(authorization,hbase_table_name)


def get_hbase_table_columns(authorization: str,  hbase_table_name: str):
    return get_table_columns(authorization,hbase_table_name)