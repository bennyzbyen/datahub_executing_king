from common_utils.all_modules import pd, os, time, mlpapp, clickhouse_connect, insert_file, logger
from params_configs.db_config import clickhouse_token, clickhouse_connect_params, cluster

class DataStorage:
    """
    DataStorage 类：负责数据清洗、多字段地理维度映射及 ClickHouse 高效上传。
    """
    def __init__(self, df_details: pd.DataFrame, df_summary: pd.DataFrame, df_ttl: pd.DataFrame, time_range: dict):
        self.df_details = df_details
        self.df_summary = df_summary
        self.df_ttl = df_ttl
        self.detail_file_path = "cal_np_details.csv"
        self.summary_file_path = "cal_np_summary.csv"
        self.ttl_file_path = "cal_np_ttl.csv"
        self.channel_file_path = "cal_b5_store_assess_channel.csv"
        
        self.time_range = time_range
        self.mars_week = self.time_range['week']

    @staticmethod
    def _retry_ck_connection(max_retries, delay_minutes, token):
        """带重试机制的连接初始化"""
        retries = 0
        while retries < max_retries:
            try:
                res_clickhouse = mlpapp.get_clickhouse_client(token)
                if res_clickhouse.get('successful') == False:
                    raise BaseException(res_clickhouse.get('err_message'))
                return res_clickhouse.get('object')
            except BaseException as e:
                logger.warning(f"Connection attempt {retries + 1} failed: {str(e)}")
                retries += 1
                if retries < max_retries:
                    time.sleep(delay_minutes * 60)
                else:
                    raise

    def _get_clickhouse_client(self, clickhouse_token):
        """初始化 ClickHouse 客户端"""
        try:
            if clickhouse_token != '':
                client = self._retry_ck_connection(5, 5, clickhouse_token)
            else:
                client = clickhouse_connect.get_client(
                    host=clickhouse_connect_params["CLICKHOUSE_HOST"],
                    user=clickhouse_connect_params["CLICKHOUSE_USER"],
                    password=clickhouse_connect_params["CLICKHOUSE_PASSWORD"],
                    port=clickhouse_connect_params["CLICKHOUSE_PORT"],
                    database=clickhouse_connect_params["CLICKHOUSE_DB"]
                )
            logger.info("ClickHouse client initialized successfully.")
            return client
        except Exception as e:
            logger.error(f"Error initializing ClickHouse client: {e}")
            raise

    def _get_geo_map_data(self):
        """
        从 CK 读取地理映射维表。
        包含 before 字段（用于关联）和 current 字段（用于结果输出）。
        """
        client = self._get_clickhouse_client('')
        source_cols = [
            'mars_region_code_before', 'mars_province_code_before', 
            'mars_city_cluster_code_before', 'mars_city_code_before',
            'mars_region_code_current', 'mars_province_code_current', 
            'mars_city_cluster_code_current', 'mars_city_code_current',
            'mars_region_name_current', 'mars_province_name_current', 
            'mars_city_cluster_name_current', 'mars_city_name_current'
        ]
        query = f"SELECT {', '.join(source_cols)} FROM sv_eo_data.mars_geo_map"
        
        try:
            logger.info("Fetching geo mapping data from ClickHouse...")
            df_geo = client.query_df(query)
            
            # 关键：根据关联键去重，确保 Left Join 是一对一或多对一，防止主表行数翻倍
            join_keys_before = [
                'mars_region_code_before', 'mars_province_code_before', 
                'mars_city_cluster_code_before', 'mars_city_code_before'
            ]
            df_geo = df_geo.drop_duplicates(subset=join_keys_before)
            return df_geo
        except Exception as e:
            logger.error(f"Error fetching geo map data: {e}")
            return None

    def _enrich_geo_info(self, df: pd.DataFrame, df_geo: pd.DataFrame):
        """
        实现 SQL 的 EXCEPT 和 多字段 LEFT JOIN 逻辑。
        """
        if df is None or df_geo is None:
            return df

        # 1. 模拟 EXCEPT 逻辑：如果主表中已存在这些 _current 字段，先将其删除
        current_cols = [
            'mars_region_code_current', 'mars_province_code_current', 
            'mars_city_cluster_code_current', 'mars_city_code_current',
            'mars_region_name_current', 'mars_province_name_current', 
            'mars_city_cluster_name_current', 'mars_city_name_current'
        ]
        cols_to_drop = [c for c in current_cols if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        # 2. 定义关联键对齐
        left_keys = ['mars_region_code', 'mars_province_code', 'mars_city_cluster_code', 'mars_city_code']
        right_keys = ['mars_region_code_before', 'mars_province_code_before', 'mars_city_cluster_code_before', 'mars_city_code_before']

        # 3. 执行 Left Join (复合主键)
        df_enriched = df.merge(df_geo, left_on=left_keys, right_on=right_keys, how='left')

        # 4. 移除从维表带过来的 _before 字段，只保留补全后的结果
        df_enriched.drop(columns=right_keys, inplace=True)

        return df_enriched

    def _upload_2_clickhouse(self, df: pd.DataFrame, file_path: str, clickhouse_table: str, client):
        """执行删除旧数据并上传新数据"""
        mars_week = self.mars_week

        if clickhouse_table in ['store_assess_channel']:
            delete_query = f"ALTER TABLE {clickhouse_table} {cluster} DELETE WHERE sku_type = 'new_sku'"
        else:
            delete_query = f"ALTER TABLE {clickhouse_table} {cluster} DELETE WHERE mars_week = '{mars_week}'"
        
        try:
            logger.info(f"Uploading to {clickhouse_table}...")
            client.command(delete_query)
            # 落地临时文件
            df.to_csv(file_path, index=False)
            insert_file(client, clickhouse_table, file_path, 
                        settings={'input_format_allow_errors_ratio': 0, 'input_format_allow_errors_num': 0})
        finally:
            # 无论成功失败，确保清理资源
            if os.path.exists(file_path):
                os.remove(file_path)

    def upload_channel_data(self, df_store_assess_channel: pd.DataFrame, client=None):
        """上传渠道评估数据，支持传入已有的 client"""
        logger.info("Start upload_channel_data")
        if df_store_assess_channel is not None:
            # 如果没传 client，则现场初始化一个（用于单独调用此方法的情况）
            if client is None:
                client = self._get_clickhouse_client(clickhouse_token)
            
            self._upload_2_clickhouse(df_store_assess_channel, self.channel_file_path, "store_assess_channel", client)
        logger.info("Finished upload_channel_data")

    def _roll_delete_old_periods(self, client, table_names: list):
        """
        滚动删除逻辑：通过计算阈值，删除旧周期数据。
        避免使用 NOT IN，改用 < 比较操作。
        """
        try:
            threshold_query = """
                SELECT MIN(period) as min_keep_period
                FROM (
                    SELECT DISTINCT period, period_start_day
                    FROM sv_eo_data.mars_calendar
                    WHERE period_start_day <= (
                        SELECT period_start_day 
                        FROM sv_eo_data.mars_calendar 
                        WHERE day = toString(today()) 
                        LIMIT 1
                    )
                    ORDER BY period_start_day DESC
                    LIMIT 13
                )
            """
            res = client.query_df(threshold_query)
            print(res)
            if res.empty or res.iloc[0, 0] is None:
                logger.warning("Could not determine the period threshold, skipping delete.")
                return

            min_keep_period = res.iloc[0, 0]
            logger.info(f"Retention threshold identified: Periods < '{min_keep_period}' will be deleted.")

            # 2. 执行删除：使用 < 运算符，效率远高于 NOT IN
            for table in table_names:
                # 确保表名带上库名
                full_table_name = f"supervisor_dashboard.{table}"
                
                # 提示：ClickHouse 的 DELETE 是异步的 Mutation
                delete_sql = f"ALTER TABLE {full_table_name} {cluster} DELETE WHERE period < '{min_keep_period}'"
                
                logger.info(f"Executing: {delete_sql}")
                client.command(delete_sql)

        except Exception as e:
            logger.error(f"Rolling delete failed: {str(e)}")

    def run(self):
        logger.info(f"Start {self.__class__.__name__}")
        client = self._get_clickhouse_client(clickhouse_token)
        
        # 1. 获取地理映射维表
        df_geo = self._get_geo_map_data()

        # 2. 定义任务清单，依次执行增强与上传
        tasks = [
            ('df_details', self.detail_file_path, "store_np_sku_details"),
            ('df_summary', self.summary_file_path, "store_np_sku_summary"),
            ('df_ttl', self.ttl_file_path, "store_np_sku_ttl")
        ]

        updated_tables = []

        for attr_name, file_path, table_name in tasks:
            df = getattr(self, attr_name)
            if df is not None:
                logger.info(f"Processing and enriching {table_name}...")
                # 执行 SQL Join 逻辑补全地理信息
                df_final = self._enrich_geo_info(df, df_geo)
                # 上传到 ClickHouse
                self._upload_2_clickhouse(df_final, file_path, table_name, client)
                updated_tables.append(table_name)

        if updated_tables:
            logger.info("开始执行 13 个周期滚动清理任务...")
            self._roll_delete_old_periods(client, updated_tables)
        
        logger.info(f"Finished {self.__class__.__name__}")