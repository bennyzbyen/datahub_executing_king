# 地理维度底表
import datetime

import pandas as pd


class GeoDim(object):
    def __init__(self, logger: any):
        self.logger = logger

    def handle(self, customer_df: pd.DataFrame, execute_king_nationwide_df: pd.DataFrame,
               geography_df: pd.DataFrame) -> pd.DataFrame:
        new_customer_df = self.basic_part(customer_df)
        final = self.cot_report_part(new_customer_df, execute_king_nationwide_df)
        final = final[list(set(final.columns) - set(
            ['mars_region_code', 'mars_region_name', 'mars_province_code', 'mars_province_name',
             'mars_city_cluster_code', 'mars_city_cluster_name', 'mars_city_name']))]
        final = pd.merge(geography_df.rename(
            columns={'RegionCode': 'mars_region_code', 'RegionName': 'mars_region_name',
                     'ProvinceCode_G': 'mars_province_code', 'ProvinceName_G': 'mars_province_name',
                     'CitygroupCode': 'mars_city_cluster_code', 'CitygroupName': 'mars_city_cluster_name',
                     'CityCode_G': 'mars_city_code', 'CityName_G': 'mars_city_name'})[
                             ['mars_region_code', 'mars_region_name', 'mars_province_code', 'mars_province_name',
                              'mars_city_cluster_code', 'mars_city_cluster_name', 'mars_city_code', 'mars_city_name']],
                         final, how='right', on='mars_city_code')
        final['update_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        final['last_update_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # final['cl_last_update_time'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for c in ['store_count_sys_reach_tt', 'sku_sale_offline_total_psall', 'store_count_sys_tt',
                  'store_count_sys_all', 'get_mh_sku_count_gum_bysku_tt', 'target_mh_sku_count_gum_bysku_tt',
                  'get_mh_sku_count_gum_bysku_ws', 'target_mh_sku_count_gum_bysku_ws', 'get_mh_sku_count_cho_bysku',
                  'target_mh_sku_count_choc_bysku', 'store_count_sys_bysku', 'gum_tt_core_sku_count_bysku_tt',
                  'cho_tt_core_sku_count_bysku_tt', 'is_fixed_cover_all']:
            final[c] = final[c].astype('Int64')
        for c in ['gi_ptd_sellout_gsv', 'gi_gt_inv_gsv', 'gi_ptd_gum_gsv_value', 'gi_ptd_gum_dts_value',
                  'gi_ptd_otc_gsv_value', 'gi_ptd_otc_gsv_vol', 'gi_ptd_otc_sellout_value', 'gi_ptd_otc_sellout_vol',
                  'sku_sale_offline_cal_psall', 'sku_sale_offline_total_psall', 'store_count_sys_reach_tt',
                  'store_count_sys_tt', 'is_fixed_cover_all', 'store_count_sys_all', 'get_mh_sku_count_gum_bysku_tt',
                  'target_mh_sku_count_gum_bysku_tt', 'get_mh_sku_count_gum_bysku_ws',
                  'target_mh_sku_count_gum_bysku_ws', 'get_mh_sku_count_cho_bysku', 'target_mh_sku_count_choc_bysku',
                  'gum_ws_core_sku_count_bysku_ws', 'store_count_sys_bysku', 'gum_tt_core_sku_count_bysku_tt',
                  'cho_tt_core_sku_count_bysku_tt']:
            final.loc[:, c] = final[c].fillna(0)
        return final

    def basic_part(self, customer_df: pd.DataFrame) -> pd.DataFrame:
        for c in ['gi_ptd_sellout_gsv', 'gi_gt_inv_gsv', 'gi_ptd_gum_gsv_value', 'gi_ptd_gum_dts_value',
                  'gi_ptd_otc_gsv_value', 'gi_ptd_otc_gsv_vol', 'gi_ptd_otc_sellout_value', 'gi_ptd_otc_sellout_vol']:
            if c in customer_df.columns:
                # customer_df[c] = customer_df[c].fillna(0.0)
                customer_df[c] = customer_df[c].astype(float)

        customer_df = customer_df.groupby(['period', 'mars_region_code', 'mars_region_name', 'mars_province_code',
                                           'mars_province_name', 'mars_city_cluster_code', 'mars_city_cluster_name',
                                           'mars_city_code',
                                           'mars_city_name']).agg({'gi_ptd_sellout_gsv': 'sum',
                                                                   'gi_gt_inv_gsv': 'sum',
                                                                   'gi_ptd_gum_gsv_value': 'sum',
                                                                   'gi_ptd_gum_dts_value': 'sum',
                                                                   'gi_ptd_otc_gsv_value': 'sum',
                                                                   'gi_ptd_otc_gsv_vol': 'sum',
                                                                   'gi_ptd_otc_sellout_value': 'sum',
                                                                   'gi_ptd_otc_sellout_vol': 'sum'}).reset_index()

        return customer_df

    def cot_report_part(self, customer_df: pd.DataFrame, execute_king_nationwide_df: pd.DataFrame) -> pd.DataFrame:
        for c in ['sku_sale_offline_cal_psall', 'sku_sale_offline_total_psall', 'store_count_sys_reach_tt',
                  'store_count_sys_tt', 'is_fixed_cover', 'store_count_sys_bysku', 'store_count_sys',
                  'get_mh_sku_count_gum_bysku',
                  'target_mh_sku_count_gum_bysku',
                  'get_mh_sku_count_cho_bysku', 'target_mh_sku_count_choc_bysku',
                  'f2963_sku', 'f3085_sku', 'f3334_sku', 'f6206_sku', 'f2962_sku', 'f3072_sku', 'f3084_sku',
                  'f3082_sku', 'f3093_sku', 'f3100_sku', 'f3101_sku', 'cho_tt_core_sku_count', 'f3317_sku'
                  ]:
            if c in execute_king_nationwide_df.columns:
                execute_king_nationwide_df[c] = execute_king_nationwide_df[c].astype(float)
                execute_king_nationwide_df[c] = execute_king_nationwide_df[c].fillna(0.0)

        sku_sale_offline_cal_psall = execute_king_nationwide_df.groupby(['mars_city_code']).agg(
            {'sku_sale_offline_cal_psall': 'sum', 'sku_sale_offline_total_psall': 'sum'}).reset_index()

        store_count_sys_reach_tt = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'TT'].groupby(['mars_city_code']).agg(
            {'store_count_sys_reach_tt': 'sum'}).reset_index()

        store_count_sys_tt = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'TT'].groupby(['mars_city_code']).agg(
            {'store_count_sys_tt': 'sum'}).reset_index()

        is_fixed_cover_all = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'TT'].groupby(['mars_city_code']).agg(
            {'is_fixed_cover': 'sum'}).reset_index().rename(columns={'is_fixed_cover': 'is_fixed_cover_all'})

        store_count_sys_all = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'TT'].groupby(['mars_city_code']).agg(
            {'store_count_sys': 'sum'}).reset_index()
        store_count_sys_all.rename(columns={'store_count_sys': 'store_count_sys_all'}, inplace=True)

        get_mh_sku_count_gum_bysku_tt = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'TT'].groupby(['mars_city_code']).agg(
            {'get_mh_sku_count_gum_bysku': 'sum'}).reset_index()
        get_mh_sku_count_gum_bysku_tt.rename(columns={'get_mh_sku_count_gum_bysku': 'get_mh_sku_count_gum_bysku_tt'},
                                             inplace=True)

        target_mh_sku_count_gum_bysku_tt = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'TT'].groupby(['mars_city_code']).agg(
            {'target_mh_sku_count_gum_bysku': 'sum'}).reset_index()
        target_mh_sku_count_gum_bysku_tt.rename(
            columns={'target_mh_sku_count_gum_bysku': 'target_mh_sku_count_gum_bysku_tt'}, inplace=True)

        get_mh_sku_count_gum_bysku_ws = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'WS'].groupby(['mars_city_code']).agg(
            {'get_mh_sku_count_gum_bysku': 'sum'}).reset_index()
        get_mh_sku_count_gum_bysku_ws.rename(columns={'get_mh_sku_count_gum_bysku': 'get_mh_sku_count_gum_bysku_ws'},
                                             inplace=True)

        target_mh_sku_count_gum_bysku_ws = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'WS'].groupby(['mars_city_code']).agg(
            {'target_mh_sku_count_gum_bysku': 'sum'}).reset_index()
        target_mh_sku_count_gum_bysku_ws.rename(
            columns={'target_mh_sku_count_gum_bysku': 'target_mh_sku_count_gum_bysku_ws'}, inplace=True)

        get_mh_sku_count_cho_bysku = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'TT'].groupby(['mars_city_code']).agg(
            {'get_mh_sku_count_cho_bysku': 'sum'}).reset_index()

        target_mh_sku_count_choc_bysku = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'TT'].groupby(['mars_city_code']).agg(
            {'target_mh_sku_count_choc_bysku': 'sum'}).reset_index()

        gum_ws_core_sku_count_bysku_ws = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'WS'].groupby(['mars_city_code']).agg(
            {'f2963_sku': 'sum',
             'f3085_sku': 'sum',
             'f3334_sku': 'sum',
             'f6206_sku': 'sum',
             'f2962_sku': 'sum',
             'f3072_sku': 'sum',
             'f3084_sku': 'sum',
             'f3082_sku': 'sum',
             'f3093_sku': 'sum',
             'f3100_sku': 'sum',
             'f3101_sku': 'sum'}).reset_index()
        gum_ws_core_sku_count_bysku_ws['gum_ws_core_sku_count_bysku_ws'] = gum_ws_core_sku_count_bysku_ws['f2963_sku'] \
                                                                           + gum_ws_core_sku_count_bysku_ws['f3085_sku'] \
                                                                           + gum_ws_core_sku_count_bysku_ws['f3334_sku'] \
                                                                           + gum_ws_core_sku_count_bysku_ws['f6206_sku'] \
                                                                           + gum_ws_core_sku_count_bysku_ws['f2962_sku'] \
                                                                           + gum_ws_core_sku_count_bysku_ws['f3072_sku'] \
                                                                           + gum_ws_core_sku_count_bysku_ws['f3084_sku'] \
                                                                           + gum_ws_core_sku_count_bysku_ws['f3082_sku'] \
                                                                           + gum_ws_core_sku_count_bysku_ws['f3093_sku'] \
                                                                           + gum_ws_core_sku_count_bysku_ws['f3100_sku'] \
                                                                           + gum_ws_core_sku_count_bysku_ws['f3101_sku']
        gum_ws_core_sku_count_bysku_ws['gum_ws_core_sku_count_bysku_ws'] = gum_ws_core_sku_count_bysku_ws[
                                                                               'gum_ws_core_sku_count_bysku_ws'] / 11
        gum_ws_core_sku_count_bysku_ws = gum_ws_core_sku_count_bysku_ws[
            ['mars_city_code', 'gum_ws_core_sku_count_bysku_ws']]

        store_count_sys_bysku = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'WS'].groupby(['mars_city_code']).agg(
            {'store_count_sys_bysku': 'sum'}).reset_index()

        gum_tt_core_sku_count_bysku_tt = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'TT'].groupby(['mars_city_code']).agg(
            {'f2963_sku': 'sum',
             'f3085_sku': 'sum',
             'f3334_sku': 'sum',
             'f6206_sku': 'sum',
             'f3317_sku': 'sum',
             'f3072_sku': 'sum',
             'f3084_sku': 'sum',
             'f3082_sku': 'sum',
             'f3093_sku': 'sum',
             'f3100_sku': 'sum',
             'f3101_sku': 'sum'}).reset_index()
        gum_tt_core_sku_count_bysku_tt['gum_tt_core_sku_count_bysku_tt'] = gum_tt_core_sku_count_bysku_tt['f2963_sku'] \
                                                                           + gum_tt_core_sku_count_bysku_tt['f3085_sku'] \
                                                                           + gum_tt_core_sku_count_bysku_tt['f3334_sku'] \
                                                                           + gum_tt_core_sku_count_bysku_tt['f6206_sku'] \
                                                                           + gum_tt_core_sku_count_bysku_tt['f3317_sku'] \
                                                                           + gum_tt_core_sku_count_bysku_tt['f3072_sku'] \
                                                                           + gum_tt_core_sku_count_bysku_tt['f3084_sku'] \
                                                                           + gum_tt_core_sku_count_bysku_tt['f3082_sku'] \
                                                                           + gum_tt_core_sku_count_bysku_tt['f3093_sku'] \
                                                                           + gum_tt_core_sku_count_bysku_tt['f3100_sku'] \
                                                                           + gum_tt_core_sku_count_bysku_tt['f3101_sku']
        gum_tt_core_sku_count_bysku_tt = gum_tt_core_sku_count_bysku_tt[
            ['mars_city_code', 'gum_tt_core_sku_count_bysku_tt']]

        cho_tt_core_sku_count_bysku_tt = execute_king_nationwide_df.loc[
            execute_king_nationwide_df['rtm_channel_name'] == 'TT'].groupby(['mars_city_code']).agg(
            {'cho_tt_core_sku_count': 'sum'}).reset_index()
        cho_tt_core_sku_count_bysku_tt.rename(columns={'cho_tt_core_sku_count': 'cho_tt_core_sku_count_bysku_tt'},
                                              inplace=True)

        final1 = sku_sale_offline_cal_psall.merge(store_count_sys_reach_tt, on='mars_city_code', how='left')
        final1 = final1.merge(store_count_sys_tt, on='mars_city_code', how='left')
        final1 = final1.merge(is_fixed_cover_all, on='mars_city_code', how='left')
        final1 = final1.merge(store_count_sys_all, on='mars_city_code', how='left')
        final1 = final1.merge(get_mh_sku_count_gum_bysku_tt, on='mars_city_code', how='left')
        final1 = final1.merge(target_mh_sku_count_gum_bysku_tt, on='mars_city_code', how='left')
        final1 = final1.merge(get_mh_sku_count_gum_bysku_ws, on='mars_city_code', how='left')
        final1 = final1.merge(target_mh_sku_count_gum_bysku_ws, on='mars_city_code', how='left')
        final1 = final1.merge(get_mh_sku_count_cho_bysku, on='mars_city_code', how='left')
        final1 = final1.merge(target_mh_sku_count_choc_bysku, on='mars_city_code', how='left')
        final1 = final1.merge(gum_ws_core_sku_count_bysku_ws, on='mars_city_code', how='left')
        final1 = final1.merge(store_count_sys_bysku, on='mars_city_code', how='left')
        final1 = final1.merge(gum_tt_core_sku_count_bysku_tt, on='mars_city_code', how='left')
        final1 = final1.merge(cho_tt_core_sku_count_bysku_tt, on='mars_city_code', how='left')

        final = customer_df.merge(final1, on='mars_city_code', how='left')
        return final
