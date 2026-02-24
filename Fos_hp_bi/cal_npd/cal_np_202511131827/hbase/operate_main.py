from hbase.operate_common import *
import pandas as pd


def count_async(authorization: str, hbase_table_name: str, row_start=None, row_stop=None, row_prefixs: [] = None):
    return get_count_async(authorization, hbase_table_name, row_start, row_stop, row_prefixs)


def count_sync(authorization: str, hbase_table_name: str, row_start=None, row_stop=None, row_prefixs: [] = None):
    return get_count_sync(authorization, hbase_table_name, row_start, row_stop, row_prefixs)


def export(authorization: str, hbase_table_name: str, columns: [], row_start: str = None, row_stop: str = None,
           row_prefixs: [] = None, hdfs_out_dir=None, export_file_format="tsv", start_time_stamp=None,end_time_stamp=None):
    result_out_dir = hbase_export_async(authorization, hbase_table_name, family_name, columns, row_start=row_start, row_stop=row_stop,
                       row_prefixs=row_prefixs, hdfs_out_dir=hdfs_out_dir, export_file_format=export_file_format, start_time_stamp=start_time_stamp, end_time_stamp=end_time_stamp)
    logger.info(f"end export file: {result_out_dir}")
    return result_out_dir


def query_df_sync(authorization: str, hbase_table_name: str, columns: [], row_start: str = None, row_stop: str = None,
                  row_prefixs: [] = None, value_filters={}, export_format="tsv", return_rowkey=True):
    # todo 这里目前是直接走data gateway
    if export_format != "tsv":
        raise Exception("export_format must be tsv")
    filter_columns = [name for name in value_filters.keys()]
    if set(filter_columns).issubset(columns) == False:
        raise BaseException(f"{filter_columns} key not in columns：{columns}")
    # 不指定索引同步查询
    logger.info(f"start export: {hbase_table_name}")
    hdfs_out_dir = hbase_export_sync(authorization, hbase_table_name, family_name, columns, row_start=row_start,
                      row_stop=row_stop, row_prefixs=row_prefixs, value_filters=value_filters, export_format=export_format)
    logger.info(f"end export: {hbase_table_name}")
    if return_rowkey:
        # gateway 同步方法返回 RowKey
        file_cols = [gateway_rowkey] + columns
    else:
        file_cols = columns
    df = get_df_by_hdfs_dir(hdfs_out_dir, file_cols, export_format)
    df.rename(columns={gateway_rowkey: default_export_row_key}, inplace=True)
    return df


def query_df_async(authorization: str, hbase_table_name: str, columns: [], row_start: str = None, row_stop: str = None,
                   row_prefixs: [] = None, value_filters={}, export_file_format="tsv", return_rowkey=True, start_time_stamp=None,end_time_stamp=None):
    filter_columns = [name for name in value_filters.keys()]
    if set(filter_columns).issubset(columns) == False:
        raise BaseException(f"{filter_columns} key not in columns：{columns}")
    # 不指定索引异步查询
    logger.info(f"start export: {hbase_table_name}")
    hdfs_out_dir = hbase_export_async(authorization, hbase_table_name, family_name, columns, row_start=row_start,
                                      row_stop=row_stop, row_prefixs=row_prefixs, value_filters=value_filters,
                                      export_file_format=export_file_format, start_time_stamp=start_time_stamp, end_time_stamp=end_time_stamp)
    logger.info(f"end scan export: {hbase_table_name}")
    if return_rowkey:
        file_cols = [default_export_row_key] + columns
    else:
        file_cols = columns
    return get_df_by_hdfs_dir(hdfs_out_dir, file_cols, export_file_format)


def insert_df_sync(authorization: str, df: pd.DataFrame, hbase_table_name: str, sep: str = default_insert_sep, **args):
    pass


def import_df_async(authorization: str, df: pd.DataFrame, hbase_table_name: str, sep: str = default_insert_sep, **args):
    # 1 生成hdfs文件
    hdfs_out_dir, hdfs_file_name = get_insert_hdfs_for_hbase(df, hbase_table_name, sep, **args)

    hbase_columns = get_hbase_columns(df.columns)
    # 2 导入hbase, 返回异步任务的tid
    return import_async(authorization, hbase_table_name, hbase_columns, sep, hdfs_out_dir, hdfs_file_name)


def insert_df_async(authorization: str, df: pd.DataFrame, hbase_table_name: str, sep: str = default_insert_sep, **args):
    # 1 生成hdfs文件
    hdfs_out_dir, hdfs_file_name = get_insert_hdfs_for_hbase(df, hbase_table_name, sep, **args)
    
    hbase_columns = get_hbase_columns(df.columns)

    # 2 导入hbase
    insert_async(authorization, hbase_table_name, hbase_columns, sep, hdfs_out_dir, hdfs_file_name)


def insert_file_sync(authorization: str, file_path: str, sep: str, columns: [] = [], hbase_table_name: str = None):
    raise BaseException(f"now not support sync insert")


def insert_file_async(authorization: str, file_path: str, sep: str, columns: [] = [], hbase_table_name: str = None):
    if default_export_row_key in columns:
        # 替换default_export_row_key
        columns = [default_insert_row_key if col == default_export_row_key else col for col in columns]
    if default_insert_row_key not in columns:
        raise BaseException(f"insert_file_async need default_insert_row_key:{default_insert_row_key}")
    hdfs_out_dir, hdfs_file_name = upload_insert_hdfs_for_hbase(file_path, hbase_table_name)

    hbase_columns = get_hbase_columns(columns)
    # 2 导入hbase
    insert_async(authorization, hbase_table_name, hbase_columns, sep, hdfs_out_dir, hdfs_file_name)
    
    
def import_file_async(authorization: str, file_path: str, sep: str, columns: [] = [], hbase_table_name: str = None):
    
    hdfs_out_dir, hdfs_file_name = upload_insert_hdfs_for_hbase(file_path, hbase_table_name)
    
    hbase_columns = get_hbase_columns(columns)
    
    # 2 导入hbase, 返回异步任务的tid
    return import_async(authorization, hbase_table_name, hbase_columns, sep, hdfs_out_dir, hdfs_file_name)
    
    

def insert_gateway_file_async(authorization: str, gateway_file_path: str, sep: str, columns: [] = [],
                              hbase_table_name: str = None):
    if default_export_row_key in columns:
        # 替换default_export_row_key
        columns = [default_insert_row_key if col == default_export_row_key else col for col in columns]
    if default_insert_row_key not in columns:
        raise BaseException(f"insert_file_async need default_insert_row_key:{default_insert_row_key}")
    file_info = gateway_file_path.split("/")
    hdfs_file_name = file_info[-1]
    hdfs_out_dir = gateway_file_path.replace(f"/{hdfs_file_name}", "")
    hbase_columns = get_hbase_columns(columns)
    # 2 导入hbase
    insert_async(authorization, hbase_table_name, hbase_columns, sep, hdfs_out_dir, hdfs_file_name)


def get_hbase_columns(columns):
    hbase_columns = [family_name + ":" + name if (name != default_insert_row_key and not name.startswith(family_name + ":")) else name for name in columns]
    return hbase_columns
    

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


def get_hbase_table_sample(authorization: str, hbase_table_name: str):
    return get_table_sample(authorization, hbase_table_name)


def get_hbase_table_columns(authorization: str, hbase_table_name: str):
    return get_table_columns(authorization, hbase_table_name)
