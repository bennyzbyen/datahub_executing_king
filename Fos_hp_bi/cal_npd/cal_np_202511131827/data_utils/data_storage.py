from common_utils.all_modules import pd, os, time, mlpapp, clickhouse_connect, insert_file, logger
from params_configs.db_config import clickhouse_token, clickhouse_connect_params, cluster

class DataStorage:
    """
    This class is used to store the data that will be used in the program.
    """
    def __init__(self, df_details: pd.DataFrame, df_summary: pd.DataFrame, df_ttl:pd.DataFrame, time_range:dict):
        self.df_details = df_details
        self.df_summary = df_summary
        self.df_ttl = df_ttl
        self.detail_file_path = "cal_np_details.csv"
        self.summary_file_path = "cal_np_summary.csv"
        self.ttl_file_path = "cal_np_ttl.csv"
        
        self.time_range = time_range
        self.mars_week = self.time_range['week']
    
    @staticmethod
    def _retry_ck_connection(max_retries, delay_minutes, token):
        retries = 0
        while retries < max_retries:
            try:
                res_clickhouse = mlpapp.get_clickhouse_client(token)
                if res_clickhouse.get('successful') == False:
                    raise BaseException(res_clickhouse.get('err_message'))
                clickhouse_client = res_clickhouse.get('object')
                return clickhouse_client
            except BaseException as e:
                logger.info(f"Connection attempt {retries + 1} failed: {str(e)}")
                retries += 1
                if retries < max_retries:
                    logger.info(f"Retrying in {delay_minutes} minutes...")
                    time.sleep(delay_minutes * 60)
                else:
                    logger.info("Max retries reached, failing...")
                    raise

    def _get_clickhouse_client(self, clickhouse_token):
        """初始化 ClickHouse 客户端"""
        try:
            if clickhouse_token != '':
                clickhouse_client = self._retry_ck_connection(5, 5, clickhouse_token)
                logger.info("ClickHouse client initialized successfully.")
            else:
                clickhouse_client =clickhouse_connect.get_client(
                host=clickhouse_connect_params["CLICKHOUSE_HOST"],
                user=clickhouse_connect_params["CLICKHOUSE_USER"],
                password=clickhouse_connect_params["CLICKHOUSE_PASSWORD"],
                port=clickhouse_connect_params["CLICKHOUSE_PORT"],
                database=clickhouse_connect_params["CLICKHOUSE_DB"]
            )
            logger.info("ClickHouse client initialized successfully.")
            return clickhouse_client
        except Exception as e:
            logger.error(f"Error initializing ClickHouse client: {e}")
            raise


    def _upload_2_clickhouse(self, df:pd.DataFrame, file_path:str, clickhouse_table:str):
        mars_week = self.mars_week
        clickhouse_client = self._get_clickhouse_client(clickhouse_token)

        if clickhouse_table in ['store_assess_channel']:
            delete_query = f"ALTER TABLE {clickhouse_table} {cluster} DELETE WHERE sku_type = 'new_sku'"
            clickhouse_client.command(delete_query)
        else:
            # 将当前mars_week的数据删除
            delete_query = f"ALTER TABLE {clickhouse_table} {cluster} DELETE WHERE mars_week = '{mars_week}'"
            clickhouse_client.command(delete_query)

        # 将数据上传到 ClickHouse
        df.to_csv(file_path, index=False)
        insert_file(clickhouse_client, clickhouse_table, file_path, settings={'input_format_allow_errors_ratio': 0, 'input_format_allow_errors_num': 0})

        # 删除临时文件
        try:
            os.remove(file_path)
        except Exception as e:
            logger.error(f"Error deleting file {file_path}: {e}")
            pass

    def upload_channel_data(self, df_store_assess_channel:pd.DataFrame):
        logger.info(f"Start upload_channel_data")
        store_assess_channel_file_path = "cal_np_store_assess_channel.csv"
        if df_store_assess_channel is not None: 
            self._upload_2_clickhouse(df_store_assess_channel, file_path=store_assess_channel_file_path, clickhouse_table="store_assess_channel")
        logger.info(f"Finished upload_channel_data")

    def run(self):
        logger.info(f"Start {self.__class__.__name__}")
        df_details = self.df_details
        df_summary = self.df_summary
        df_ttl = self.df_ttl
        detail_file_path = self.detail_file_path
        summary_file_path = self.summary_file_path
        ttl_file_path = self.ttl_file_path
        logger.info("DataStorage is running")
        if df_details is not None:
            self._upload_2_clickhouse(df_details, file_path=detail_file_path, clickhouse_table="store_np_sku_details")
        if df_summary is not None:
            self._upload_2_clickhouse(df_summary, file_path=summary_file_path, clickhouse_table="store_np_sku_summary")
        if df_ttl is not None:
            self._upload_2_clickhouse(df_ttl, file_path=ttl_file_path, clickhouse_table="store_np_sku_ttl")
        
        logger.info(f"Finished {self.__class__.__name__}")       