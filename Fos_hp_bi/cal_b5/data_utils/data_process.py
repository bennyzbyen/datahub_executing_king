from common_utils.all_modules import pd, np, gc, logger
from params_configs.col_config import b5_summary_basic_columns, b5_detail_basic_columns

class DataProcess:
    def __init__(self, df_origin:pd.DataFrame, df_sku:pd.DataFrame, time_range:dict, params:dict):
        logger.info("Initializing DataProcess...")
        self.sku_map = params['sku_map']
        self.sku_ttl_filter = params['sku_ttl_filter']
        self.sku_columns = list(self.sku_map.keys())
        self.sku_cal_range = params['sku_cal_range']
        self.df_origin = df_origin
        self.df_sku = df_sku
        self.time_range = time_range


    def get_base_df(self)->pd.DataFrame:
        mars_week = self.time_range['week']
        sku_columns = self.sku_columns
        df = self.df_origin.copy()
        df['mars_week'] = mars_week
        df = df[
            (df['channel_name'].isin(['现代渠道','传统渠道'])) &
            (df['channel_level2_name'] != '仓储式卖场') &
            (df['mars_region_name'] != '总部订单大区')
        ]
        df_origin = df[b5_detail_basic_columns+sku_columns]

        new_sku_columns = []
        # 遍历sku_columns，创建新列并将其添加到列表中
        for col in sku_columns:
            new_col = col.replace('f', 'y', 1)  # 只替换第一个 'f'
            df_origin[new_col] = df_origin[col]
            new_sku_columns.append(new_col)

        return df_origin, new_sku_columns
    
    def cal_r6p_sku(self, new_sku_columns:list)->pd.DataFrame:
        logger.info("Calculating R6P...")
        sku_columns = self.sku_columns
        sku = self.df_sku.copy()

        rename_map = {col: 'y' + col[1:] for col in sku_columns}
        sku.rename(columns=rename_map, inplace=True)

        # 将bySKU截P报表中4个P对应的SKU分销结果相加，若≥1，说明门店R6P有分销该SKU，值记1，否则记0。
        sku[new_sku_columns] = sku[new_sku_columns].apply(lambda x: x.astype(float).astype('Int64'))
        grouped_sum = sku.groupby('code')[new_sku_columns].sum()
        boolean_indicator = grouped_sum >= 1    
        sku_result = boolean_indicator.astype(int).reset_index()

        sku_result['y8606'] = np.where((sku_result['y8606'] + sku_result['y7604']) > 0, 1, 0)

        logger.info(f"Finished calculating R6P")
        return sku_result
    
    def __filetered_df(self, df:pd.DataFrame)->pd.DataFrame:
        logger.info("Filtering data...")
        # 条件 ②: 门店有效性为“1”
        condition2 = df['state'] == '1'

        # 条件 ③: 剔除客户系列为：步步高、千惠、家乐福
        chain_brand_name_list = ['步步高', '千惠', '家乐福']
        condition3 = ~df['chain_brand_name'].isin(chain_brand_name_list)

        # 条件 ④: 剔除全国总部为：永辉生活、华润万家-高端店
        nation_hq_name_list = ['永辉生活', '华润万家-高端店']
        condition4 = ~df['nation_hq_name'].isin(nation_hq_name_list)

        # 条件 ①: 最后一访为“1”（仅一级门店类型 = 现代渠道 且 RTM渠道 ≠ FT-Non KA 的门店有此过滤条件）
        applicability_cond1 = (df['channel_name'] == '现代渠道') & (df['rtm_channel_name'] != 'FT-Non KA')
        condition1 = (~applicability_cond1) | (df['is_last_visit'] == '1') # 如果不适用条件1，或者适用条件1且最后一访为1

        # 结合所有过滤条件
        final_filter = condition1 & condition2 & condition3 & condition4
        df_filtered = df[final_filter].copy()
        
        logger.info(f"Filtered data shape: {df_filtered.shape}")
        return df_filtered


    def _cal_channel_categories(self, df: pd.DataFrame, is_sku: bool = False) -> pd.DataFrame:
        """
        Calculate cal_store_channel_category and cal_channel_category based on the given conditions.
        """
        mapping_table = self.sku_ttl_filter
        df_processed = df.copy()

        # Calculate cal_store_channel_category
        conditions_store_channel = [
            (df_processed['channel_level2_name'] == '大卖场') & (df_processed['rtm_channel_name'] != 'FT-Non KA'),  # Rule 1
            (df_processed['channel_level2_name'] == '超级市场') & (df_processed['rtm_channel_name'] != 'FT-Non KA'), # Rule 2
            (df_processed['channel_level2_name'] == '小型超市') & (df_processed['rtm_channel_name'] != 'FT-Non KA'), # Rule 3
            (df_processed['channel_level2_name'] == '便利店') & (df_processed['rtm_channel_name'] != 'FT-Non KA'),   # Rule 4
            (df_processed['rtm_channel_name'] == 'FT-Non KA'),                                          # Rule 5
            (df_processed['rtm_channel_name'] == 'FT-TT')                                               # Rule 6
        ]
        choices_store_channel = [
            'Hyper',
            'Super',
            'Mini',
            'CVS',
            'Non KA MT',
            'TT'
        ]
        df_processed['cal_store_channel_category'] = np.select(conditions_store_channel, choices_store_channel, default=None)

        if not is_sku:
            for column_name, values_to_match_cal_channel in mapping_table.items():
                condition = (df_processed['cal_store_channel_category'].isin(values_to_match_cal_channel))
                df_processed.loc[condition, column_name] = 'remove'

        # Calculate cal_channel_category
        conditions_for_chain = ['Hyper', 'Super', 'Mini', 'CVS']
        conditions_for_non_chain = ['Non KA MT', 'TT']

        conditions_channel_category = [
            df_processed['cal_store_channel_category'].isin(conditions_for_chain),
            df_processed['cal_store_channel_category'].isin(conditions_for_non_chain)
        ]
        choices_channel_category = ['Chain', 'Non Chain']
        df_processed['cal_channel_category'] = np.select(conditions_channel_category, choices_channel_category, default=None)

        return df_processed

    def _cal_channel_categories_chain_ttl(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate cal_store_channel_category and cal_channel_category based on the given conditions.
        """
        mapping_table = self.sku_ttl_filter
        df_processed = df.copy()

        # Calculate cal_store_channel_category
        conditions_store_channel = [
            (df_processed['channel_level2_name'] == '大卖场') & (df_processed['rtm_channel_name'] != 'FT-Non KA'),  # Rule 1
            (df_processed['channel_level2_name'] == '超级市场') & (df_processed['rtm_channel_name'] != 'FT-Non KA'), # Rule 2
            (df_processed['channel_level2_name'] == '小型超市') & (df_processed['rtm_channel_name'] != 'FT-Non KA'), # Rule 3
            (df_processed['channel_level2_name'] == '便利店') & (df_processed['rtm_channel_name'] != 'FT-Non KA'),   # Rule 4
            (df_processed['rtm_channel_name'] == 'FT-Non KA'),                                          # Rule 5
            (df_processed['rtm_channel_name'] == 'FT-TT')                                               # Rule 6
        ]
        choices_store_channel = [
            'Hyper',
            'Super',
            'Mini',
            'CVS',
            'Non KA MT',
            'TT'
        ]
        df_processed['cal_store_channel_category'] = np.select(conditions_store_channel, choices_store_channel, default=None)

        for column_name, values_to_match_cal_channel in mapping_table.items():
            condition = (df_processed['cal_store_channel_category'].isin(values_to_match_cal_channel))
            df_processed.loc[condition, column_name] = 'remove'

        chain_condition = (df_processed['cal_store_channel_category'].isin(['Hyper','Super','Mini','CVS']))
        df_processed.loc[chain_condition, 'cal_store_channel_category'] = 'Chain TTL'

        no_chain_condition = (df_processed['cal_store_channel_category'].isin(['Non KA MT','TT']))
        df_processed.loc[no_chain_condition, 'cal_store_channel_category'] = 'Non Chain TTL'

        # Calculate cal_channel_category
        conditions_for_chain = ['Chain TTL']
        conditions_for_non_chain = ['Non Chain TTL']

        conditions_channel_category = [
            df_processed['cal_store_channel_category'].isin(conditions_for_chain),
            df_processed['cal_store_channel_category'].isin(conditions_for_non_chain)
        ]
        choices_channel_category = ['Chain', 'Non Chain']
        df_processed['cal_channel_category'] = np.select(conditions_channel_category, choices_channel_category, default=None)

        return df_processed
    
    def _cal_channel_categories_ttl(self, df: pd.DataFrame) -> pd.DataFrame:
        mapping_table = self.sku_ttl_filter
        df_processed = df.copy()

        # Calculate cal_store_channel_category
        conditions_store_channel = [
            (df_processed['channel_level2_name'] == '大卖场') & (df_processed['rtm_channel_name'] != 'FT-Non KA'),  # Rule 1
            (df_processed['channel_level2_name'] == '超级市场') & (df_processed['rtm_channel_name'] != 'FT-Non KA'), # Rule 2
            (df_processed['channel_level2_name'] == '小型超市') & (df_processed['rtm_channel_name'] != 'FT-Non KA'), # Rule 3
            (df_processed['channel_level2_name'] == '便利店') & (df_processed['rtm_channel_name'] != 'FT-Non KA'),   # Rule 4
            (df_processed['rtm_channel_name'] == 'FT-Non KA'),                                          # Rule 5
            (df_processed['rtm_channel_name'] == 'FT-TT')                                               # Rule 6
        ]
        choices_store_channel = [
            'Hyper',
            'Super',
            'Mini',
            'CVS',
            'Non KA MT',
            'TT'
        ]
        df_processed['cal_store_channel_category'] = np.select(conditions_store_channel, choices_store_channel, default=None)

        for column_name, values_to_match_cal_channel in mapping_table.items():
            condition = (df_processed['cal_store_channel_category'].isin(values_to_match_cal_channel))
            df_processed.loc[condition, column_name] = 'remove'


        # set cal_store_channel_category to 'TTL'
        df_processed['cal_store_channel_category'] = 'TTL'

        # set cal_channel_category to 'TTL'
        df_processed['cal_channel_category'] = 'TTL'

        return df_processed

    def __cal_summary_kpi_columns(self, df:pd.DataFrame):
        logger.info("Calculating summary KPI columns...")
        df_cal = df.copy()
        # 计算cal_ncd_category 和 cal_ncd
        df_cal['cal_ncd_category'] = np.where(df_cal['rtm_channel_name'] == 'NKA', 'NCD', 'NNCD')
        df_cal['cal_ncd'] = df_cal['cal_ncd_category'] + df_cal['mars_region_name']

        # 计算cal_channel_category 和 cal_store_channel_category
        df_cal = self._cal_channel_categories(df_cal)
        df_chain_ttl = self._cal_channel_categories_chain_ttl(df_cal)
        df_ttl = self._cal_channel_categories_ttl(df_cal)

        logger.info("Finished calculating summary KPI columns")
        return df_cal, df_chain_ttl, df_ttl
    
    def __cal_groupby_columns(self, df:pd.DataFrame, new_sku_columns:list, is_sku_filter:bool=False)->pd.DataFrame:
        """
        :param df:
        :param new_sku_columns:
        :param is_sku_filter: 是否是ttl
        :return:
        """
        logger.info("Calculating groupby columns...")
        sku_columns = self.sku_columns
        sku_map = self.sku_map
        tmp_b5_summary_columns = list(b5_summary_basic_columns)
        tmp_new_sku_columns = list(new_sku_columns)
        df_cal = df.copy()

        # 进行sku行转列
        melted_df_new = df_cal.melt(
                id_vars=tmp_b5_summary_columns,
                value_vars=tmp_new_sku_columns,
                value_name='b5_sku_count',
                var_name='b5_sku'
            )
        
        melted_df_origin = df_cal.melt(
            id_vars=tmp_b5_summary_columns,
            value_vars=sku_columns,
            value_name='b5_sku_r3p_count',
            var_name='b5_sku_r3p'
        )

        
        melted_df_origin = melted_df_origin.reset_index(drop=True)
        melted_df_new = melted_df_new.reset_index(drop=True)

        final_melted_df = pd.concat([
            melted_df_new,
            melted_df_origin['b5_sku_r3p_count']
        ], axis=1)
        final_melted_df['b5_sku'] = final_melted_df['b5_sku'].str.replace('y', 'f', 1)

        if is_sku_filter:
            final_melted_df = final_melted_df[final_melted_df['b5_sku_count'] != 'remove']

        del melted_df_new
        del melted_df_origin
        gc.collect()
        

        # 汇总store_count_sys, b5_sku_count
        final_melted_df['store_count_sys'] = final_melted_df['store_count_sys'].astype(int)
        final_melted_df['b5_sku_count'] = final_melted_df['b5_sku_count'].astype(float).astype(int)
        final_melted_df['b5_sku_r3p_count'] = final_melted_df['b5_sku_r3p_count'].astype(float).astype(int)

        tmp_b5_summary_columns.append('b5_sku')
        tmp_b5_summary_columns.remove('store_count_sys')
        final_result = final_melted_df.groupby(tmp_b5_summary_columns, dropna=False)[['store_count_sys', 'b5_sku_count','b5_sku_r3p_count']].sum().reset_index()

        # 合并 mars_city_cluster_store_count_sys
        mars_city_store_count_sys = final_result.groupby(['mars_city_cluster_code','cal_store_channel_category', 'b5_sku'], dropna=False)['store_count_sys'].sum().reset_index(name='mars_city_cluster_store_count_sys')
        final_result = pd.merge(final_result, mars_city_store_count_sys, on=['mars_city_cluster_code','cal_store_channel_category','b5_sku'], how='left')

        final_result['b5_sku'] = final_result['b5_sku'].map(sku_map)

        logger.info("Groupby columns calculated.")

        return final_result
         

    def cal_summary(self, df:pd.DataFrame, new_sku_columns:list)->pd.DataFrame:
        logger.info("Calculating summary KPI columns...")
        df_filtered = self.__filetered_df(df)
        df_cal, df_chain_ttl, df_ttl = self.__cal_summary_kpi_columns(df_filtered)

        logger.info(f'start to calculate sku')
        result_df = self.__cal_groupby_columns(df_cal, new_sku_columns, is_sku_filter=True)
        logger.info(f'finish to calculate sku')

        logger.info(f'start to calculate chain_ttl and ttl')
        result_df_chain_ttl = self.__cal_groupby_columns(df_chain_ttl, new_sku_columns, is_sku_filter=True)
        result_df_chain_ttl['table_type'] = 'chain_ttl'
        result_df_ttl = self.__cal_groupby_columns(df_ttl, new_sku_columns, is_sku_filter=True)
        result_df_ttl['table_type'] = 'ttl'
        result_df_ttl_combo = pd.concat([result_df_chain_ttl, result_df_ttl], ignore_index=True)
        logger.info(f'finish to calculate chain_ttl and ttl')

        result_df = result_df[~result_df['b5_sku'].isin(['益达粒装OTC薄荷味 14g'])]
        result_df_ttl_combo = result_df_ttl_combo[~result_df_ttl_combo['b5_sku'].isin(['益达粒装OTC薄荷味 14g'])]
        return result_df, result_df_ttl_combo

    def cal_sku(self, df:pd.DataFrame, r6p_sku_df:pd.DataFrame, new_sku_columns:list)->pd.DataFrame:
        sku_columns = list(new_sku_columns)
        r3p_rename_dict = {col: f'{col}_r3p_temp' for col in sku_columns}
        r6p_rename_dict = {col: f'{col}_r6p_temp' for col in sku_columns}

        # 合并后会产生的临时列名列表
        temp_cols = [f'{col}_r3p_temp' for col in sku_columns] + [f'{col}_r6p_temp' for col in sku_columns]

        # cal r3p sku
        result_df_details = df.rename(columns=r3p_rename_dict)

        # cal r6p sku
        r6p_merge_keys = ['code']
        r6p_cols_to_merge = r6p_merge_keys + sku_columns

        result_df_details = pd.merge(
            result_df_details,
            r6p_sku_df[r6p_cols_to_merge].rename(columns=r6p_rename_dict),
            on=r6p_merge_keys,
            how='left'
        )

        condition_r3p = (result_df_details['channel_name'] == '现代渠道') & \
                        (result_df_details['rtm_channel_name'] != 'FT-Non KA')

        condition_r6p = (result_df_details['channel_name'].isin(['现代渠道','传统渠道'])) & \
                        (result_df_details['rtm_channel_name'].isin(['FT-Non KA', 'FT-TT']))
        
        conditions_list = [condition_r3p, condition_r6p]
        
        for sku_col in sku_columns:
            values_to_assign_sku = [
                result_df_details[f'{sku_col}_r3p_temp'],
                result_df_details[f'{sku_col}_r6p_temp']
            ]

            result_df_details[sku_col] = np.select(
                conditions_list,
                values_to_assign_sku,
                default=np.nan
            )

        result_df_details = result_df_details.drop(columns=temp_cols)

        # 2025P10 新增cal_channel_category 渠道分类 & cal_store_channel_category 门店类型分类
        result_df_details = self._cal_channel_categories(result_df_details, is_sku=True)

        return result_df_details


    def run(self):
        logger.info(f"Start {self.__class__.__name__}")

        # 1.计算基础dataframe(包含r3p)
        df_origin, new_sku_columns = self.get_base_df()

        # 2. 计算r6p sku
        r6p_sku_df = self.cal_r6p_sku(new_sku_columns)

        # 3. 计算sku达成明细
        result_df_details = self.cal_sku(df_origin, r6p_sku_df, new_sku_columns) 

        # 4.计算b5分销达成汇总
        result_df_summary, result_df_ttl_combo = self.cal_summary(result_df_details, new_sku_columns)

        logger.info(f"Finished {self.__class__.__name__}")  
        return result_df_details, result_df_summary, result_df_ttl_combo