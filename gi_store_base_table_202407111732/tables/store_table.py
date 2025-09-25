# 门店维度底表
import datetime

import pandas as pd


class StoreDim(object):
    def __init__(self, logger: any):
        self.logger = logger

    def handle(self, all_p_df, bysku_df, cvs_df, mini_df, tt_df,
               ws_df, ps_all_df, proj_east_df, proj_north_df, proj_tt_df):
        logger = self.logger
        logger.info("开始生成门店维度报表")
        logger.info("开始拼接主数据和其他报表数据")
        t9 = self.root_data(all_p_df, bysku_df, cvs_df, mini_df, tt_df,
                            ws_df, ps_all_df, proj_east_df, proj_north_df, proj_tt_df)
        t9 = self.store_count_sys_reach_tt(t9)
        final = self.cho_tt_core_sku_count(t9)
        # final = self.others_columns(t9)
        columns_type_int = ['wal_mart', 'has_storefront', 'state', 'lbcx', 'store_cheetah', 'store_kk',
                            'store_count_sys', 'is_last_visit', 'visit_in_current_p', 'visit_uid',
                            'store_count_sys_reach_tt', 'cho_tt_core_sku_count']
        for col in columns_type_int:
            final[col] = final[col].apply(lambda x: self.trans_to_str(x))
        return final

    def trans_to_str(self, x):
        if pd.isna(x):
            return None
        else:
            return str(int(float(x)))

    @staticmethod
    def rename_df(df: pd.DataFrame, df_type: str) -> pd.DataFrame:
        if df_type == 'all_p_df':
            return df.rename(columns={'mars_region_code': 'mars_region_code',
                                      'mars_province_code': 'mars_province_code',
                                      'mars_city_cluster_code': 'mars_city_cluster_code',
                                      'mars_city_code': 'mars_city_code',
                                      'region_code': 'region_code',
                                      'chain_brand_code': 'chain_brand_code',
                                      'nation_hq_code': 'nation_hq_code',
                                      'city_hq_code_visit': 'city_hq_code_visit',
                                      'ka_type_code': 'ka_type_code',
                                      'channel_code': 'channel_code',
                                      'channel_level2_code': 'channel_level2_code',
                                      'store_channel_code': 'store_channel_code',
                                      'rtm_channel_code': 'rtm_channel_code',
                                      'period': 'period',
                                      'mars_region_name': 'mars_region_name',
                                      'mars_province_name': 'mars_province_name',
                                      'mars_city_cluster_name': 'mars_city_cluster_name',
                                      'mars_city_name': 'mars_city_name',
                                      'region_name': 'region_name',
                                      'chain_brand_name': 'chain_brand_name',
                                      'nation_hq_name': 'nation_hq_name',
                                      'ka_type_name': 'ka_type_name',
                                      'store_level': 'store_level',
                                      'city_hq_code': 'city_hq_code',
                                      'city_hq_name': 'city_hq_name',
                                      'rtm_channel_name': 'rtm_channel_name',
                                      'code': 'code',
                                      'bd_env_name': 'bd_env_name',
                                      'store_name': 'store_name',
                                      'channel_name': 'channel_name',
                                      'channel_level2_name': 'channel_level2_name',
                                      'store_channel_name': 'store_channel_name',
                                      'store_level_visit': 'store_level_visit',
                                      'city_hq_name_visit': 'city_hq_name_visit',
                                      'wal_mart': 'wal_mart',
                                      'biz_scope': 'biz_scope',
                                      'has_storefront': 'has_storefront',
                                      'employee_type_name': 'employee_type_name',
                                      'employee_code': 'employee_code',
                                      'employee_name': 'employee_name',
                                      'salesman_code': 'salesman_code',
                                      'salesman_name': 'salesman_name',
                                      'supervisor_code': 'supervisor_code',
                                      'emp_channel_name': 'emp_channel_name',
                                      'emp_type_name': 'emp_type_name',
                                      'supervisor_name': 'supervisor_name',
                                      'state': 'state',
                                      'lbcx': 'lbcx',
                                      'store_cheetah': 'store_cheetah',
                                      'store_seg': 'store_seg',
                                      'digital': 'digital',
                                      'store_kk': 'store_kk',
                                      'manager_code': 'manager_code',
                                      'manager_name': 'manager_name',
                                      'mg_channel_name': 'mg_channel_name',
                                      'mg_type_name': 'mg_type_name',
                                      'first_emp_supervisor_code': 'first_emp_supervisor_code',
                                      'first_emp_supervisor_name': 'first_emp_supervisor_name',
                                      'second_emp_supervisor_code': 'second_emp_supervisor_code',
                                      'second_emp_supervisor_name': 'second_emp_supervisor_name',
                                      'visit_date': 'visit_date',
                                      'is_assess': 'is_assess',
                                      'first_approved_timestamp': 'first_approved_timestamp',
                                      'ttf': 'ttf',
                                      'store_count_sys': 'store_count_sys',
                                      'is_last_visit': 'is_last_visit',
                                      'is_fixed_cover': 'is_fixed_cover',
                                      'visit_in_current_p': 'visit_in_current_p',
                                      'visit_uid': 'visit_uid'})
        elif df_type == 'bysku_df':
            return df.rename(columns={'store_count_sys': 'store_count_sys_bysku',
                                      'get_mh_sku_count_gum': 'get_mh_sku_count_gum_bysku',
                                      'get_mh_sku_count_choc': 'get_mh_sku_count_cho_bysku',
                                      'get_mh_sku_count_mint': 'get_mh_sku_count_mint_bysku',
                                      'get_mh_sku_count_fc': 'get_mh_sku_count_fc_bysku',
                                      'target_mh_sku_count_gum': 'target_mh_sku_count_gum_bysku',
                                      'target_mh_sku_count_choc': 'target_mh_sku_count_choc_bysku',
                                      'target_mh_sku_count_mint': 'target_mh_sku_count_mint_bysku',
                                      'target_mh_sku_count_fc': 'target_mh_sku_count_fc_bysku',
                                      'f2963': 'f2963_sku',
                                      'f3085': 'f3085_sku',
                                      'f3334': 'f3334_sku',
                                      'f6206': 'f6206_sku',
                                      'f3317': 'f3317_sku',
                                      'f3072': 'f3072_sku',
                                      'f3084': 'f3084_sku',
                                      'f3082': 'f3082_sku',
                                      'f3093': 'f3093_sku',
                                      'f3100': 'f3100_sku',
                                      'f3101': 'f3101_sku',
                                      'sale_resource': 'sale_resource_bysku',
                                      'f2962': 'f2962_sku',
                                      'f2994': 'f2994_sku',
                                      'f3328': 'f3328_sku',
                                      'f3151': 'f3151_sku',
                                      'f3338': 'f3338_sku',
                                      'f3150': 'f3150_sku',
                                      'f2953': 'f2953_sku',
                                      'f3994': 'f3994_sku',
                                      'f3112': 'f3112_sku',
                                      'f3052': 'f3052_sku',
                                      'f3003': 'f3003_sku',
                                      'f3086': 'f3086_sku',
                                      'f2961': 'f2961_sku',
                                      'f2990': 'f2990_sku',
                                      'f2983': 'f2983_sku',
                                      'f2984': 'f2984_sku',
                                      'f3142': 'f3142_sku',
                                      'f6702': 'f6702_sku',
                                      'f3130': 'f3130_sku',
                                      'f5715': 'f5715_sku',
                                      'f6405': 'f6405_sku'
                                      })
        elif df_type == 'cvs_df':
            return df.rename(columns={'wwy_manual_co_count': 'wwy_manual_co_count_cvs',
                                      'manual_co_count': 'manual_co_count_cvs',
                                      'wwy_self_co_count': 'wwy_self_co_count_cvs',
                                      'self_co_count': 'self_co_count_cvs',
                                      'has_floor3_cover_num': 'has_floor3_cover_num_cvs',
                                      'dd_mars_floor3_cover': 'dd_mars_floor3_cover_cvs',
                                      'dining_mars_cover': 'dining_mars_cover_cvs',
                                      'drinks_mars_cover': 'drinks_mars_cover_cvs'})
        elif df_type == 'mini_df':
            return df.rename(columns={'wwy_manual_co_count': 'wwy_manual_co_count_minijy',
                                      'manual_co_count': 'manual_co_count_minijy',
                                      'wwy_self_co_count': 'wwy_self_co_count_minijy',
                                      'self_co_count': 'self_co_count_minijy',
                                      'has_floor3_cover_num': 'has_floor3_cover_num_minijy'})
        elif df_type == 'tt_df':
            return df.rename(columns={'store_count_sys': 'store_count_sys_tt',
                                      'hexagon_layer_count': 'hexagon_layer_count_tt',
                                      'aust_display_wwy_count': 'aust_display_wwy_count_tt',
                                      'aust_display_choc_count': 'aust_display_choc_count_tt'})
        elif df_type == 'ws_df':
            return df.rename(columns={'display_gum_sale_count_all': 'display_gum_sale_count_all_ws'})
        elif df_type == 'ps_all_df':
            return df.rename(columns={'sku_sale_offline_cal': 'sku_sale_offline_cal_psall',
                                      'sku_sale_offline_total': 'sku_sale_offline_total_psall'})
        else:
            return df

    def root_data(self, all_p_df, bysku_df, cvs_df, mini_df, tt_df,
                  ws_df, ps_all_df, proj_east_df, proj_north_df, proj_tt_df) -> pd.DataFrame:
        all_p_df = StoreDim.rename_df(all_p_df, 'all_p_df')
        t1 = all_p_df.merge(StoreDim.rename_df(bysku_df, 'bysku_df'), on='code', how='left')
        t2 = t1.merge(StoreDim.rename_df(cvs_df, 'cvs_df'), on='code', how='left')
        t3 = t2.merge(StoreDim.rename_df(mini_df, 'mini_df'), on='code', how='left')
        t4 = t3.merge(StoreDim.rename_df(tt_df, 'tt_df'), on='code', how='left')
        t5 = t4.merge(StoreDim.rename_df(ws_df, 'ws_df'), on='code', how='left')
        t6 = t5.merge(StoreDim.rename_df(ps_all_df, 'ps_all_df'), on='code', how='left')
        t7 = t6.merge(StoreDim.rename_df(proj_east_df, 'proj_east_df'), on='code', how='left')
        t8 = t7.merge(StoreDim.rename_df(proj_north_df, 'proj_north_df'), on='code', how='left')
        t9 = t8.merge(StoreDim.rename_df(proj_tt_df, 'proj_tt_df'), on='code', how='left')

        return t9

    def store_count_sys_reach_tt(self, t9: pd.DataFrame) -> pd.DataFrame:
        """
        "注意：以下步骤只考核TT渠道，其他渠道为空（TT渠道只有1/0）
       若门店为TT渠道，则进行下列步骤
       第一步：计算门店的六角架、澳洲架的达标情况，最大值为1
       1、按门店分层的逻辑计算

       1.1 六角架：按门店分层的逻辑计算达标门店数，最大值为1
       ①multiIf(({门店分层}=‘金’) and (({[收-呈]六角架累计层数tt})>=5), 1,
       ({门店分层}=‘银’) and (({[收-呈]六角架累计层数tt})>=4), 1,
       ({门店分层}=‘铜’) and (({[收-呈]六角架累计层数tt})>=3), 1, 0)。
       结果标记为=A

       1.2 澳洲架：按门店分层，计算两个澳洲相加的达标门店数，最大值为1
       ②multiIf(({门店分层}=‘金’) and (({[收-呈]澳洲架陈列箭牌产品层数tt}+{[收-呈]澳洲架陈列巧克力产品层数tt})>=4), 1,
       ({门店分层}=‘银’) and (({[收-呈]澳洲架陈列箭牌产品层数tt}+{[收-呈]澳洲架陈列巧克力产品层数tt})>=3), 1,
       ({门店分层}=‘铜’) and (({[收-呈]澳洲架陈列箭牌产品层数tt}+{[收-呈]澳洲架陈列巧克力产品层数tt})>=2), 1, 0)。
       结果标记为=B

       1.3 以上六角架澳洲架结果相加大于0，就为1
       ③if(（A+B）>0,1,0)

       若第一步结果为1，就不进行下面步骤；若第一步结果为0，继续下面步骤

       2、以下是按门店的大区为东区西区北区，判断一级门店类型、KA类型，进行下面步骤

       2.1 若以上步骤结果为0，则计算符合过滤条件的字段值
       过滤条件：大区=业务东区and一级门店类型=传统渠道andKA类型=空白
       不符合过滤条件=0，符合则=if(是否有蓝绿双塔陈列hsp_field061>0,1,0)

       2.2 若以上步骤结果为0，则计算符合过滤条件的字段值
       过滤条件：大区=业务西区and一级门店类型=传统渠道andKA类型=空白
       不符合过滤条件=0，符合则=
       if(收银口玛氏箭牌产品层数陈列使用MW开放架co_mars_mw_open_rack>0,1,0)

       2.3若以上步骤结果为0，则计算符合过滤条件的字段值
       过滤条件：大区=业务北区and一级门店类型=传统渠道andKA类型=空白
       不符合过滤条件=0，符合则=if((乐高架陈列个数hsp_field062  +  组合架个数hsp_field063  +  玛氏箭牌OTB个数mars_wrigley_otb_num)>0,1,0)

       以上操作步骤按顺序计算，一旦门店结果=1，则结果=1，不进行后续步骤"

        :param t9:
        :return:
        """
        t9['store_count_sys_reach_tt'] = None
        t9['A'] = 0
        t9['B'] = 0
        t9['ka_type_name'] = t9['ka_type_name'].fillna('')

        # 创建字段备份
        for c in ['co_mars_mw_open_rack', 'hsp_field061', 'hsp_field062', 'hsp_field063', 'mars_wrigley_otb_num']:
            t9[f"{c}_bak"] = t9[c]

        for c in ['co_mars_mw_open_rack', 'hsp_field061', 'hsp_field062', 'hsp_field063', 'mars_wrigley_otb_num']:
            if c in t9.columns:
                t9[c] = t9[c].fillna('0')
                t9[c] = t9[c].astype(float).astype('Int64')
            else:
                t9[c] = 0

        # Step1
        # 六角架
        cond = t9['rtm_channel_code'] == 'TT'
        cond1 = (t9['store_seg'] == '金') & (t9['hexagon_layer_count_tt'].astype(float).astype('Int64') >= 5)
        t9.loc[cond & cond1, 'A'] = 1

        cond2 = (t9['store_seg'] == '银') & (t9['hexagon_layer_count_tt'].astype(float).astype('Int64') >= 4)
        t9.loc[cond & cond2, 'A'] = 1

        cond3 = (t9['store_seg'] == '铜') & (t9['hexagon_layer_count_tt'].astype(float).astype('Int64') >= 3)
        t9.loc[cond & cond3, 'A'] = 1

        # 澳洲架
        cond4 = (t9['store_seg'] == '金') & \
                ((t9['aust_display_wwy_count_tt'].astype(float).astype('Int64') + t9[
                    'aust_display_choc_count_tt'].astype(float).astype(
                    'Int64')) >= 4)
        t9.loc[cond & cond4, 'B'] = 1

        cond4 = (t9['store_seg'] == '银') & \
                ((t9['aust_display_wwy_count_tt'].astype(float).astype('Int64') + t9[
                    'aust_display_choc_count_tt'].astype(float).astype(
                    'Int64')) >= 3)
        t9.loc[cond & cond4, 'B'] = 1

        cond4 = (t9['store_seg'] == '铜') & \
                ((t9['aust_display_wwy_count_tt'].astype(float).astype('Int64') + t9[
                    'aust_display_choc_count_tt'].astype(float).astype(
                    'Int64')) >= 2)
        t9.loc[cond & cond4, 'B'] = 1

        t9.loc[cond & (t9['A'] + t9['B'] > 0), 'store_count_sys_reach_tt'] = 1
        t9.loc[cond & (t9['A'] + t9['B'] <= 0), 'store_count_sys_reach_tt'] = 0

        # Step2
        cond1_1 = (t9['rtm_channel_code'] == 'TT') & (t9['store_count_sys_reach_tt'] == 0)
        cond5 = (t9['mars_region_name'] == '业务东区') & (t9['channel_name'] == '传统渠道') & (t9['ka_type_name'] == '')
        t9.loc[cond1_1 & cond5, 'store_count_sys_reach_tt'] = t9.loc[cond1_1 & cond5].apply(
            lambda row: 1 if row['hsp_field061'] > 0 else 0, axis=1)

        cond1_1 = (t9['rtm_channel_code'] == 'TT') & (t9['store_count_sys_reach_tt'] == 0)
        cond5 = (t9['mars_region_name'] == '业务西区') & (t9['channel_name'] == '传统渠道') & (t9['ka_type_name'] == '')
        t9.loc[cond1_1 & cond5, 'store_count_sys_reach_tt'] = t9.loc[cond1_1 & cond5].apply(
            lambda row: 1 if row['co_mars_mw_open_rack'] > 0 else 0, axis=1)

        cond1_1 = (t9['rtm_channel_code'] == 'TT') & (t9['store_count_sys_reach_tt'] == 0)
        cond5 = (t9['mars_region_name'] == '业务北区') & (t9['channel_name'] == '传统渠道') & (t9['ka_type_name'] == '')
        t9.loc[cond1_1 & cond5, 'store_count_sys_reach_tt'] = t9.loc[cond1_1 & cond5].apply(
            lambda row: 1 if row['hsp_field062'] + row['hsp_field063'] + row['mars_wrigley_otb_num'] > 0 else 0, axis=1)

        t9.drop(columns=['A', 'B'], inplace=True)

        # 恢复字段备份
        for c in ['co_mars_mw_open_rack', 'hsp_field061', 'hsp_field062', 'hsp_field063', 'mars_wrigley_otb_num']:
            t9.drop(columns=[c])
            t9[c] = t9[f"{c}_bak"]

        return t9

    def cho_tt_core_sku_count(self, t9: pd.DataFrame) -> pd.DataFrame:
        """
        "注意：以下步骤只考核TT渠道，其他渠道为空

        若门店为TT渠道，则进行下列步骤

        1、其中（【德芙丝滑牛奶巧克力43克】+【德芙香浓黑巧克力43克】+【德芙白巧克力43克】）>0,1,0
        (说明：德芙丝滑牛奶巧克力43克、德芙香浓黑巧克力43克、德芙白巧克力43克属于一个产品组，相加=0则值为0，相加>0则值为1)

        2、其中(【士力架花生20克】+【士力架花生460克桶装】)>0,1,0
        (说明：士力架花生20克和士力架花生460克桶装属于一个产品组，相加=0则值为0，相加>0则值为1)

        3、其他5个sku直接求和得出每项sku的激活门店数
        【德芙丝滑牛奶巧克力14克】+【力架花生51克】+【脆香米24克】+【脆香米12g 48*16】+【M&Ms牛奶巧克力豆30.6克筒装】

        汇总步骤1、2、3的结果

        说明：最大结果为7"

        "德芙丝滑牛奶巧克力43克bysku：f2994_sku
        德芙香浓黑巧克力43克bysku：f3052_sku
        德芙白巧克力43克bysku：f3003_sku
        德芙丝滑牛奶巧克力14克bysku：f3328_sku
        士力架花生51克bysku：f3151_sku
        士力架花生20克bysku：f3338_sku
        士力架花生460克桶装bysku：f3150_sku
        脆香米24克bysku：f2953_sku
        脆香米12g 48*16bysku：f3994_sku
        M&Ms牛奶巧克力豆30.6克筒装bysku：f3112_sku"

        :param t9:
        :return:
        """
        need_columns = ['f2994_sku', 'f3052_sku', 'f3003_sku', 'f3328_sku', 'f3151_sku',
                        'f3338_sku', 'f3150_sku', 'f2953_sku', 'f3994_sku', 'f3112_sku']

        for c in need_columns:
            t9[f"{c}_bak"] = t9[c]

        for c in need_columns:
            if c in t9.columns:
                t9[c] = t9[c].fillna('0')
                t9[c] = t9[c].astype(float).astype('Int64')
            else:
                t9[c] = 0

        t9['cho_tt_core_sku_count'] = None
        t9['s1'] = 0
        t9['s2'] = 0
        t9['s3'] = 0

        cond = t9['rtm_channel_code'] == 'TT'
        t9.loc[cond, 's1'] = t9.loc[cond].apply(
            lambda row: 1 if row['f2994_sku'] + row['f3052_sku'] + row['f3003_sku'] > 0 else 0, axis=1)

        t9.loc[cond, 's2'] = t9.loc[cond].apply(lambda row: 1 if row['f3338_sku'] + row['f3150_sku'] > 0 else 0, axis=1)

        t9.loc[cond, 's3'] = t9.loc[cond].apply(
            lambda row: row['f3328_sku'] + row['f3151_sku'] + row['f2953_sku'] + row['f3994_sku'] + row[
                'f3112_sku'], axis=1)

        t9.loc[cond, 'cho_tt_core_sku_count'] = t9.loc[cond]['s1'] + t9.loc[cond]['s2'] + t9.loc[cond]['s3']
        t9.loc[cond, 'cho_tt_core_sku_count'] = t9.loc[cond].apply(lambda row: min(row['cho_tt_core_sku_count'], 7),
                                                                   axis=1)

        t9.drop(columns=['s1', 's2', 's3'], inplace=True)

        # 恢复字段备份
        for c in need_columns:
            t9.drop(columns=[c])
            t9[c] = t9[f"{c}_bak"]

        return t9

    def others_columns(self, t9: pd.DataFrame) -> pd.DataFrame:
        return t9.assign(cl_last_update_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
