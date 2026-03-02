from common import ClientWrapper
from clickhouse_connect.driver.tools import insert_file
from all_configs.database_config import *

import pandas as pd
import threading


class CLickHouseOperation:
    def __init__(self):
        self.clients = ClientWrapper(client_type='clickhouse', clickhouse_token=clickhouse_token)
        self.clickhouse_client = self.clients.clickhouse_client

    def insert_to_clickhouse(self, file_path, clickhouse_table):
        clickhouse_client = self.clickhouse_client
        logger.info(f"Start uploading to clickhouse")
        insert_file(clickhouse_client, f'{clickhouse_table}', f'{file_path}', settings={'input_format_allow_errors_ratio': 0, 'input_format_allow_errors_num': 0})
        logger.info(f"Finished uploading to clickhouse")

    def delete_by_partition(self, clickhouse_table, partition):
        clickhouse_client = self.clickhouse_client
        if cluster == '':
            sql = f"ALTER TABLE {clickhouse_table} DROP PARTITION '{partition}';"
        else:
            sql = f"ALTER TABLE {clickhouse_table} ON CLUSTER cl_1shards_2replicas DROP PARTITION '{partition}';"
        logger.info(f"{clickhouse_table}开始删除{partition}分区")
        logger.info(f"sql:{sql}")
        clickhouse_client.command(sql)
        logger.info(f"{clickhouse_table}删除{partition}分区成功")



class HBaseOperation:
    def __init__(self):
        self.clients = ClientWrapper(client_type='gateway')
        self.fs_client = self.clients.fs_client
        self.hbase_client = self.clients.hbase_client


    def read_hbase_2_df(self, table_name:str, columns:list, row_start:str=None, row_stop:str=None)->pd.DataFrame:
        hbase_client = self.hbase_client
        row_prefixs = [str(i) for i in range(10)]
        df = hbase_client.query_df(hbase_table_name=table_name, columns=columns, row_start=row_start, row_stop=row_stop, row_prefixs=row_prefixs)
        return df


    def read_multiple_tables_2_df(self, table_names: list, columns_map: dict, row_start: str, row_stop: str) -> dict:
        df_map = {}
        for table_name in table_names:
            columns_to_export = columns_map.get(table_name)
            df = self.read_hbase_2_df(table_name=table_name, columns=columns_to_export, row_start=row_start, row_stop=row_stop)
            df_map[table_name] = df

        return df_map


    def read_multiple_tables_2_df_multithread(self, table_names: list, columns_map: dict, row_start: str, row_stop: str) -> dict:
        logger.info(f"Start exporting source data")
        df_map = {}
        threads = []

        def export_table_thread(table_name):
            columns_to_export = columns_map.get(table_name)
            df = self.read_hbase_2_df(table_name=table_name, columns=columns_to_export, row_start=row_start, row_stop=row_stop)
            df.drop(columns=['rowkey'], inplace=True)
            df_map[table_name] = df

        for table_name in table_names:
            thread = threading.Thread(target=export_table_thread, args=(table_name,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
        logger.info(f"Finished exporting source data")
        return df_map


# from all_configs.key_config import *
# if __name__ == '__main__':
#     exporter = HBaseOperation()
#     table_names = ["table1", "table2", "table3"]
#     #  定义 columns_map 字典，为每个表指定不同的列名列表
#     columns_map_example = table_mapping

#     # 使用非多线程版本
#     dfs1 = exporter.read_multiple_tables_2_df(
#         table_names=table_names,
#         columns_map=columns_map_example,
#         row_start="2025P03",
#         row_stop="2025P03Z"
#     )

#     # 使用多线程版本
#     dfs2 = exporter.read_multiple_tables_2_df_multithread(
#         table_names=table_names,
#         columns_map=columns_map_example,
#         row_start="2025P03",
#         row_stop="2025P03Z"
#     )