from all_configs.database_config import *
from all_configs.key_config import *
from database_op import HBaseOperation, CLickHouseOperation
from common import return_pipeline_result, ImprovedEmailSender

import pandas as pd

class EXECUTE_KING(object):
    def __init__(self, params:dict):
        self.params = params
        self.table_mapping = params['table_mapping']
        self.column_mapping = params['column_mapping']
        self.table_list = params['source_table_list']
        self.ext_table_name = params['ext_table_name']
        self.ext_col_mapping = params.get('ext_col_mapping', None)
        self.ext_table_columns = self.ext_col_mapping.keys()

        self.ck_op = CLickHouseOperation()


    def get_execute_range(self, calendar_table = "l0_mdp.mars_calendar")->list:
        hbase_op = HBaseOperation()
        table_mapping = self.table_mapping

        logger.info(f"Start getting execute range")
     
        YESTERDAY = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        calendar_df = hbase_op.read_hbase_2_df(table_name=calendar_table, columns=table_mapping['l0_mdp.mars_calendar'])
        row = calendar_df.loc[calendar_df['dataid'] == YESTERDAY].iloc[0]
        PERIOD = row['m_year'] + row['m_period'].zfill(2)
        PERIOD_OUTPUT = row['m_year'] + 'P' + row['m_period'].zfill(2)
 
        logger.info(f"Running params: YESTERDAY: {YESTERDAY}, PERIOD: {PERIOD_OUTPUT}, ENV: {env}")
        return [PERIOD_OUTPUT]
    

    def _export_all_source_data(self, period:str, metrics_list:list)-> Tuple[dict, List[Dict]]:
        hbase_op = HBaseOperation()
        table_list = self.table_list
        table_mapping = self.table_mapping
        column_mapping = self.column_mapping
        
        df = hbase_op.read_multiple_tables_2_df_multithread(table_names=table_list, columns_map=table_mapping,row_start=period, row_stop=f"{period}Z")
        ######################################################
        # df = {}
        # for table in table_list:
        #     temp_df = pd.read_csv(f"{table}.csv", dtype = str)
        #     df[table] = temp_df
        ######################################################

        for table in list(column_mapping.keys()):
            df[table] = df[table].rename(columns=column_mapping[table])

        for table in table_list:
            # df[table].to_csv(f'{table}.csv', index = False)
            metrics = {
                "mysql_table":f"{table}",
                "period":f"{period}",
                "size":f"{len(df[table])}",
            }
            metrics_list.append(metrics)
        return df, metrics_list
    

    def _merge_dataframes(self, df:pd.DataFrame, df_source:pd.DataFrame, table_list:list)->pd.DataFrame:
        logger.info(f"Start merging dataframes")
        for table_name in table_list:
            table_df = df_source[table_name]
            df = pd.merge(df, table_df, on=['period', 'code'], how='left')
        logger.info(f"Finished merging dataframes")
        return df
    

    def _division(self, df:pd.DataFrame, numerator_col:str, denominator_col:str, result_col:str, zero_division_value=np.nan)->pd.DataFrame:
        """
        使用 np.where 对 DataFrame 中的字段进行条件除法计算。
        当除数为 0 或为空时，结果字段显示为空 (NaN)。

        Args:
            df (pd.DataFrame): 输入的 DataFrame。
            numerator_col (str): 被除数字段的名称。
            denominator_col (str): 除数字段的名称。
            result_col (str): 结果存储字段的名称。
            zero_division_value: 当除数为 0 或空时，结果字段的值 (默认为 np.nan)。

        Returns:
            pd.DataFrame: 修改后的 DataFrame。
        """
        denominator_float = df[denominator_col].astype(float)
        numerator_float = df[numerator_col].astype(float)
        condition = (denominator_float != 0) & pd.notna(denominator_float)
        df[result_col] = np.where(
            condition,
            (numerator_float / denominator_float).round(4),
            zero_division_value
        )
        return df
    

    def _col_calculate(self, df:pd.DataFrame)->pd.DataFrame:

        for cal_info in cal_dict:
            numerator_col = cal_info['numerator_col']
            denominator_col = cal_info['denominator_col']
            result_col = cal_info['result_col']
            df = self._division(df, numerator_col, denominator_col, result_col)

        return df
    

    def _delete_code(self, df_merge:pd.DataFrame)->pd.DataFrame:
        hbase_op = HBaseOperation()
        fs = hbase_op.fs_client
        delete_code_list = fs.listdir(fs_save_dir)
        df_total_delete_code = pd.DataFrame()
        for delete_code_file in delete_code_list:
            logger.info(delete_code_file)
            fs.copy_to_local(f'{fs_save_dir}{delete_code_file}', delete_code_file)
            df = pd.read_csv(delete_code_file, dtype=str)
            df_total_delete_code = pd.concat([df_total_delete_code, df], ignore_index=True)
            os.remove(delete_code_file)
        df_total_delete_code = df_total_delete_code.drop_duplicates(subset=['code'])
        logger.info(f"delete code size: {len(df_total_delete_code)}")
        df_merge = df_merge[~df_merge['code'].isin(df_total_delete_code['code'].tolist())]

        return df_merge
    

    def _upload_2_ck(self, file_path:str, clickhouse_table:str, period:str):
        ck_op = self.ck_op
        ck_op.delete_by_partition(clickhouse_table,period)
        ck_op.insert_to_clickhouse(file_path, clickhouse_table)
        try:
            os.remove(file_path)
        except:
            pass
    
    def _calculate_blacklist(self, df: pd.DataFrame, ext_df: pd.DataFrame) -> pd.DataFrame:
        """
        根据给定的条件计算并生成 blacklist 字段
        """
        result_col = 'blacklist'
        df[result_col] = 0
        
        # 条件 B：关键词匹配 (ole 或 blt，不区分大小写)
        cond_b = df['store_name'].str.contains('ole|blt', case=False, na=False) | \
                 df['city_hq_name'].str.contains('ole|blt', case=False, na=False)
        df.loc[cond_b, result_col] = 1
        
        # 条件 A：关联匹配 (与外部数据比对)
        base_keys = ['chain_brand_name', 'nation_hq_name', 'city_hq_name', 'code']
        period_keys = ['period'] + base_keys
        
        # 1. 外部数据中 period 有值：使用 period + 基础字段 进行匹配
        ext_with_p = ext_df[ext_df['period'].notna()][period_keys].drop_duplicates()
        ext_with_p['__match__'] = 1
        merged_p = df.merge(ext_with_p, on=period_keys, how='left')
        df.loc[merged_p['__match__'] == 1, result_col] = 1
        
        # 2. 外部数据中 period 为空：仅使用 基础字段 进行降级匹配
        ext_no_p = ext_df[ext_df['period'].isna()][base_keys].drop_duplicates()
        ext_no_p['__match__'] = 1
        merged_no_p = df.merge(ext_no_p, on=base_keys, how='left')
        df.loc[merged_no_p['__match__'] == 1, result_col] = 1

        return df
    
    def _fetch_ext_df(self, clickhouse_table:str, columns:list, ext_col_mapping:dict)->pd.DataFrame:
        ck_op = self.ck_op
        df = ck_op.fetch_ext_df(clickhouse_table, columns)
        df = df.rename(columns=ext_col_mapping)
        return 
    
    def _get_geo_map_data(self):
        ck_op = self.ck_op
        return ck_op._get_geo_map_data()
    
    def _enrich_geo_info(self, df: pd.DataFrame, df_geo: pd.DataFrame):
        """
        实现 SQL 的 EXCEPT 和 多字段 LEFT JOIN 逻辑。
        """
        if df is None or df_geo is None:
            return df

        # 1. EXCEPT 逻辑：移除已有的 _current 字段，确保数据源自维表映射
        current_cols = [
            'mars_region_code_current', 'mars_province_code_current', 
            'mars_city_cluster_code_current', 'mars_city_code_current',
            'mars_region_name_current', 'mars_province_name_current', 
            'mars_city_cluster_name_current', 'mars_city_name_current'
        ]
        cols_to_drop = [c for c in current_cols if c in df.columns]
        if cols_to_drop:
            df = df.drop(columns=cols_to_drop)

        # 2. 定义多字段关联键
        left_keys = ['mars_region_code', 'mars_province_code', 'mars_city_cluster_code', 'mars_city_code']
        right_keys = ['mars_region_code_before', 'mars_province_code_before', 'mars_city_cluster_code_before', 'mars_city_code_before']

        # 3. 执行 Left Join
        df_enriched = df.merge(df_geo, left_on=left_keys, right_on=right_keys, how='left')

        # 4. 清理维表带过来的 before 关联键字段，仅保留 current 结果
        df_enriched.drop(columns=right_keys, inplace=True)

        return df_enriched


    def execute_king(self, period_list:list, metrics_list:list, clickhouse_table:str, output_path:str)->dict:
        table_list = self.table_list
        ext_table_name = self.ext_table_name
        ex_table_columns = self.ext_table_columns
        ext_col_mapping = self.ext_col_mapping

        for period_str in period_list:
            st = datetime.datetime.now()
            # 导出数据源
            df_source, metrics_list = self._export_all_source_data(period_str, metrics_list)

            # 开始拼接（只含join）
            main_table_name = table_list[0] # zo_all_2025_p为主表
            main_df = df_source[main_table_name].copy() 
            main_df = main_df[main_df['mars_region_name'] != '总部订单大区']
            df_merge = self._merge_dataframes(main_df, df_source, table_list[1:])

            # 计算部分
            df_merge = self._col_calculate(df_merge)

            # 剔除门店
            if period_str > '2025P10':
                df_merge = self._delete_code(df_merge)

            # 剔除客户清单(HS+MC)
            ext_df = self._fetch_ext_df(ext_table_name, ex_table_columns, ext_col_mapping)
            logger.info(f"Start calculating blacklist")
            df_merge = self._calculate_blacklist(df_merge, ext_df)
            logger.info(f"Finished calculating blacklist")

            # 拼接地域信息
            df_geo = self._get_geo_map_data()
            df_merge = self._enrich_geo_info(df_merge, df_geo)

            # 保存结果
            df_merge.to_csv(output_path, index=False)

            # 上传clickhouse
            self._upload_2_ck(output_path, clickhouse_table, period_str)


            ed = datetime.datetime.now()
            metrics = {
                "clickhouse_table":f"{clickhouse_table}",
                "result_size":f"{len(df_merge)}",
                "duration":f"{ed-st}"
            }
            metrics_list.append(metrics)
        
        return return_pipeline_result(metrics_list)
    

    def execute(self)->dict:
        params = self.params
        period_list = params.get('period', None)
        clickhouse_table = params['clickhouse_table']
        receiver_emails = params.get('receiver_emails', ["zhangbenyan@inkstone.tech"])
        output_path = "./executing_king.csv"
        metrics_list = []

        try:
            # 获取Period
            if not period_list:
                period_list = self.get_execute_range()

            logger.info(f"Period list: {period_list}")
            

            # 执行
            result = self.execute_king(period_list, metrics_list, clickhouse_table, output_path)

        except Exception as e:
            error_prefix = "执行失败"
            logger.error(error_prefix)
            error_detail = traceback.format_exc()
            email_subject = f'事故 {env}-Execute King {clickhouse_table} 执行失败'
            email_sender = ImprovedEmailSender("inkstone.cot@effem.com", receiver_emails, email_subject)
            error_message = email_sender.email_body(error_prefix, error_detail)
            email_sender.send_email(error_message)
            raise RuntimeError(f"{error_detail}: {str(e)}") from e
                

        return result
        



