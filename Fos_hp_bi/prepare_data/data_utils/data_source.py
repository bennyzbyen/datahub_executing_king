from common_utils.all_modules import pd, threading, GateWayClient, Tuple, Dict, logger
from params_configs.db_config import *
from params_configs.col_config import hbase_init_export_table, hbase_daily_export_table


class DataSource:
    def __init__(self, time_range:dict, params:dict):
        self.params = params
        self.APP_KEY = app_key
        self.APP_SECRET = app_secret
        self.FS_ROOT_DIR = fs_root_dir

        self.export_range = time_range


    def read_hbase_2_df(self, table_name:str, columns:list, row_start:str=None, row_stop:str=None) -> pd.DataFrame:
        gateway_client =GateWayClient(self.APP_KEY, self.APP_SECRET)
        hbase_client = gateway_client.getHbaseClient(self.FS_ROOT_DIR)
        row_prefixs = [str(i) for i in range(10)]
        df = hbase_client.query_df(hbase_table_name=table_name, columns=columns, row_start=row_start, row_stop=row_stop, row_prefixs=row_prefixs)
        return df
    
    def upload_df_2_fs(self, df:pd.DataFrame, period:str) -> None:
        gateway_client =GateWayClient(self.APP_KEY, self.APP_SECRET)
        fs = gateway_client.getFsClient()
        file_path = f"zo_bysku_detail_{period}.csv.gz"
        df.to_csv(file_path, index=False, compression='gzip')
        fs.copy_from_local(file_path, f"{fs_save_bysku_dir}/{file_path}", overwrite=True)
        os.remove(file_path)


    def fetch_hbase_tables_df_threaded(self, table_name:str, columns_to_export:list, period_list:list):
        threads = []

        def export_table_thread(table_name, period):

            df = self.read_hbase_2_df(table_name=table_name, columns=columns_to_export, row_start=period, row_stop=f"{period}Z")
            df.drop(columns=['rowkey'], inplace=True)
            # for testing
            # file_name = f"{table_name}_{row_start}_{row_stop}.csv"
            # df.to_csv(file_name, index=False)
            # df = pd.read_csv(file_name, dtype=str, usecols=columns_to_export)
            
            self.upload_df_2_fs(df, period)
            logger.info(f"Exporting {table_name} for {period}")

        for period in period_list:
            thread = threading.Thread(target=export_table_thread, args=(table_name, period,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()
        logger.info(f"Finished exporting source data")
    
        

    def run(self) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], pd.DataFrame]:
        params = self.params
        export_range = self.export_range
        export_mode = params['export_mode']
        hbase_export_cols = params['hbase_export_cols']
        specific_range = params.get('specific_range', None)
        specific_table = params.get('specific_table', None)
        logger.info(f"Start {self.__class__.__name__}")
        

        if export_mode == 'init':

            if specific_range is not None and specific_table is not None:
                # 指定时间的同时，也要指定表
                period_list = specific_range
                table_name = specific_table
                self.fetch_hbase_tables_df_threaded(table_name=table_name, columns_to_export=hbase_export_cols, period_list=period_list)

            else:
                hbase_table_names = hbase_init_export_table
                for table_name in hbase_table_names:
                    logger.info(f"Exporting {table_name}")

                    if table_name == 'l2_cot_perfect_store.zo_bysku_detail_2025_p':
                        period_list = export_range['bysku2025_period']
                    else:
                        period_list = export_range['bysku2024_period']
                    self.fetch_hbase_tables_df_threaded(table_name=table_name, columns_to_export=hbase_export_cols, period_list=period_list)
        
        elif export_mode == 'daily':
            period_list = [export_range['period']]
            table_name = hbase_daily_export_table
            self.fetch_hbase_tables_df_threaded(table_name=table_name, columns_to_export=hbase_export_cols, period_list=period_list)

        logger.info(f"Finished {self.__class__.__name__}")
        