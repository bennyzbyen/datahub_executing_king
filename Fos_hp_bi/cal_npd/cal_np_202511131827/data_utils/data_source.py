from common_utils.all_modules import pd, GateWayClient, Dict, logger
from params_configs.db_config import *
from params_configs.col_config import source_table_name


class DataSource:
    def __init__(self, time_range:dict, params:dict):
        self.file_type = params.get('file_type', 'csv')
        self.sku_map = params['sku_map']
        self.sku_columns = list(self.sku_map.keys())
        self.sku_cal_range = params.get('sku_cal_range', None)
        self.APP_KEY = app_key
        self.APP_SECRET = app_secret
        self.FS_ROOT_DIR = fs_root_dir
        self.FS_BYSKU_DIR = fs_save_bysku_dir

        self.time_range= time_range

    def filter_files_by_period_list(self, period_list: list) -> list:
        """
        根据提供的子字符串列表过滤文件名列表。

        Args:
            search_substrings: 包含用于过滤的子字符串的列表 (e.g., ['report', 'csv']).

        Returns:
            一个包含匹配文件名的列表。
        """
        fs_bysku_dir = self.FS_BYSKU_DIR

        gateway_client = GateWayClient(self.APP_KEY, self.APP_SECRET)
        fs = gateway_client.getFsClient()
        filenames_list = fs.listdir(fs_bysku_dir) # 获取fs所有bysku文件名

        filtered_files = []
        for filename in filenames_list:
            for term in period_list:
                if term in filename:
                    filtered_files.append(filename)
                    # 找到一个匹配项即可，避免重复添加
                    break
        return filtered_files

    @staticmethod
    def read_csv_df(file_path: str, usecols:list = None) -> pd.DataFrame:
        return pd.read_csv(file_path, dtype=str, usecols=usecols)
    
    def read_pqrquet_df(self, file_path: str) -> pd.DataFrame:
        return pd.read_parquet(file_path)

    @staticmethod
    def read_gzip_df(file_path: str, usecols:list = None)->pd.DataFrame:
        return pd.read_csv(file_path, dtype=str, usecols=usecols, compression='gzip')
    
    def read_fs_2_df(self, fs_file_path: str, file_path: str, usecols:list = None) -> pd.DataFrame:
        file_type = self.file_type
        gateway_client = GateWayClient(self.APP_KEY, self.APP_SECRET)
        fs = gateway_client.getFsClient()
        if_exist = fs.exists(fs_file_path)
        if if_exist:
            fs.copy_to_local(fs_file_path, file_path)
        else:
            logger.error(f"{fs_file_path}-->文件不存在")
            raise BaseException(f"{fs_file_path}-->文件不存在")
        
        if file_type == 'csv':
            df = self.read_csv_df(file_path, usecols)
        elif file_type == 'csv.gz':
            df = self.read_gzip_df(file_path, usecols)
        else:
            raise BaseException(f"file_type {file_type} not support")
        
        try:
            os.remove(file_path)
        except:
            logger.error(f"删除文件失败{file_path}")
            pass
        return df

    def fetch_bysku_df(self)-> pd.DataFrame:
        time_range = self.time_range
        sku_cal_range = self.sku_cal_range
        fs_bysku_dir = self.FS_BYSKU_DIR
        current_period = time_range['period']

        cal_range_list = list(self.sku_cal_range.keys())
        
        df_comb = pd.DataFrame()
        for range in cal_range_list:
            period_list = time_range[range]
            sku_list = sku_cal_range[range]

            all_fs_file_path = self.filter_files_by_period_list(period_list)

        
            df_sku = pd.DataFrame()
            for fs_file_name in all_fs_file_path:
                fs_file_path = f"{fs_bysku_dir}/{fs_file_name}"
                df = self.read_fs_2_df(fs_file_path, fs_file_name, usecols=['code']+sku_list)
                df[sku_list] = df[sku_list].fillna(0)
                logger.info(f"读取文件{fs_file_path}成功, size: {len(df)}")
                df_sku = pd.concat([df_sku, df], ignore_index=True)

            df_comb = pd.concat([df_comb, df_sku], ignore_index=True)
        
        return df_comb
    
    def fetch_origin_df(self) -> pd.DataFrame:
        time_range = self.time_range
        fs_bysku_dir = self.FS_BYSKU_DIR
        file_type = self.file_type
        sku_columns = self.sku_columns
        current_period = time_range['period']
        
        fs_file_name = f"{source_table_name}_{current_period}.{file_type}"
        fs_file_path = f"{fs_bysku_dir}/{fs_file_name}"
        df = self.read_fs_2_df(fs_file_path, fs_file_name)
        df[sku_columns] = df[sku_columns].fillna(0)
        logger.info(f"读取文件{fs_file_path}成功, size: {len(df)}")

        return df
        

    def run(self) -> Dict[str, pd.DataFrame]:
        logger.info(f"Start {self.__class__.__name__}")

        df_origin = self.fetch_origin_df()
        df_sku = self.fetch_bysku_df()

        return df_origin, df_sku
        