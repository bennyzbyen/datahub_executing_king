from common_utils.all_modules import logger, datetime, pd, traceback, os, GateWayClient
from common_utils.common import return_pipeline_result, ImprovedEmailSender
from data_utils.data_source import DataSource
from data_utils.data_process import PIPELINE_ACTIVATE
from params_configs.db_config import env, app_key, app_secret, fs_save_calendar_dir


class PREPARE_DATA(object):
    def __init__(self, params:dict):
        self.params = params
        self.APP_KEY = app_key
        self.APP_SECRET = app_secret
        self.current_date = params.get("current_date", None)
        self.calendar_file_path = "mars_calendar.csv"
        self.time_range = self.__get_execute_range()
        
        
    def fetch_calendar_df(self)-> pd.DataFrame:
        file_path = self.calendar_file_path
        gateway_client = GateWayClient(self.APP_KEY, self.APP_SECRET)
        fs = gateway_client.getFsClient()
        fs_file_path  = f"{fs_save_calendar_dir}/{file_path}"
        if_exist = fs.exists(fs_file_path)
        if if_exist:
            fs.copy_to_local(fs_file_path, file_path)
        else:
            # 如果日历不存在，则发送邮件通知
            logger.error("日历文件不存在，请检查！")
            raise BaseException("日历文件不存在，请检查！")
        df = pd.read_csv(file_path, dtype=str)
        try:
            os.remove(file_path)
        except:
            pass
        return df

    def __get_execute_range(self) -> dict:
        current_date = self.current_date
        """获取执行范围"""
        logger.info(f"Start getting execute range")
        
        if current_date is None:
            current_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        currentday = pd.to_datetime(current_date).strftime("%Y%m%d")
        yester_day = (pd.to_datetime(current_date) - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        yesterday = (pd.to_datetime(current_date) - datetime.timedelta(days=1)).strftime("%Y%m%d")

        
        calendar_df = self.fetch_calendar_df()
        calendar_df['formatted_dataid'] = pd.to_datetime(calendar_df['dataid'], format='%Y%m%d').dt.strftime('%Y-%m-%d')
        
        ## 获取今日时间的P & 年份
        # print(currentday)
        row = calendar_df.loc[calendar_df['dataid'] == currentday].iloc[0]
        m_period = row['m_period']
        m_year = row['m_year']
        m_week = row['m_week']

        ## 获取P
        p = m_year + 'P' + m_period.zfill(2)
        week = m_year + 'P' + m_period + 'W' + m_week

        ## 获取2024P09 至今的 period list
        df_period = calendar_df.drop_duplicates(subset=['m_year','m_period'], keep='first').copy()
        df_period.sort_values(by=['dataid'], inplace=True)
        df_period = df_period.reset_index()
        df_period = df_period.iloc[0: df_period[(df_period['m_year'] == m_year) & (df_period['m_period'] == m_period)].index[0]+1]
        df_period = df_period[df_period['dataid'] >= '20240801']
        df_period['m_period'] = df_period['m_period'].astype(int)
        period_list = [f"{row['m_year']}P{row['m_period']:02d}" for index, row in df_period.iterrows()]

        ## R6P
        r6p_list = period_list[-4:]

        ## YTD
        ytd_list = [period for period in period_list if period.startswith(m_year)]

        ## bysku2025 period
        bysku2025_index = period_list.index('2025P03')
        bysku2025_period = period_list[bysku2025_index:]

        ## bysku2024 period
        bysku2024_index_start = period_list.index('2024P09')
        bysku2024_index_end = period_list.index('2025P02')
        bysku2024_period = period_list[bysku2024_index_start:bysku2024_index_end+1]
        

        logger.info(f"""current_date: {current_date}, yester_day: {yester_day},currentday:{currentday},\n 
                    yesterday: {yesterday}, period: {p}, week:{week}, r6p: {r6p_list}, ytd:{ytd_list}, \n
                    bysku2025_period:{bysku2025_period}, bysku2024_period:{bysku2024_period}""")

        time_range = {"period": p, "week":week, "current_date": current_date, "yester_day": yester_day, 
                    "currentday": currentday, "yesterday": yesterday, "r6p": r6p_list, "ytd": ytd_list, 
                    "bysku2025_period": bysku2025_period, "bysku2024_period": bysku2024_period}
        
        # current_date: 2025-05-08, yester_day: 2025-05-07,currentday:20250508, 
        #         yesterday: 20250507, period: 2025P05, week:2025P5W3, r6p: ['2025P02', '2025P03', '2025P04', '2025P05'], ytd:['2025P01', '2025P02', '2025P03', '2025P04', '2025P05'],
        #         bysku2025_period:['2025P03', '2025P04', '2025P05'], bysku2024_period:['2024P09', '2024P10', '2024P11', '2024P12', '2024P13', '2025P01', '2025P02']

        return time_range

    def execute(self):
        params = self.params
        time_range = self.time_range
        receiver_emails = params.get('receiver_emails', ["zhangbenyan@inkstone.tech"])
        is_run_pipeline = params.get('is_run_pipeline', False)

        try:
            """执行任务"""
            # 读取数据源
            start_time = datetime.datetime.now()
            if not is_run_pipeline:
                DATASOURCE = DataSource(time_range, params)
                DATASOURCE.run()
            end_time = datetime.datetime.now()
            data_source_time = end_time - start_time
            logger.info(f"Data source time: {data_source_time}")

            # 调度水线任务
            start_time = datetime.datetime.now()
            DATAPROCESS = PIPELINE_ACTIVATE()
            DATAPROCESS.run()
            end_time = datetime.datetime.now()


            task_result = [{
                "data_source_time": str(data_source_time),
            }]
        except Exception as e:
            error_prefix = "执行失败"
            logger.error(error_prefix)
            error_detail = traceback.format_exc()
            email_subject = f'事故 {env}-Fos-HQ BI Execute King 执行失败'
            email_sender = ImprovedEmailSender("inkstone.cot@effem.com", receiver_emails, email_subject)
            error_message = email_sender.email_body(error_prefix, error_detail)
            email_sender.send_email(error_message)
            raise RuntimeError(f"{error_detail}: {str(e)}") from e
        return return_pipeline_result(task_result)