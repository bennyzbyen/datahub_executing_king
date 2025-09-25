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
    

    def _upload_2_ck(self, file_path:str, clickhouse_table:str, period:str):
        ck_op = CLickHouseOperation()
        ck_op.delete_by_partition(clickhouse_table,period)
        ck_op.insert_to_clickhouse(file_path, clickhouse_table)
        try:
            os.remove(file_path)
        except:
            pass
    


    def execute_king(self, period_list:list, metrics_list:list, clickhouse_table:str, output_path:str)->dict:
        table_list = self.table_list

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
        



