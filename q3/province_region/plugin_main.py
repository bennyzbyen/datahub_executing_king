import re
import os
import sys
import json
import datetime
import traceback
import numpy as np
import pandas as pd
import clickhouse_connect
from loguru import logger
from decimal import Decimal, ROUND_HALF_UP

# env = 'DEV'
# env = 'QA'
# env = 'PROD'


env = os.environ.get('running_env') or 'uat'
logger.info(f"running env： {env}")

if env == 'uat':
    clickhouse_host = '10.216.3.92'
    clickhouse_port = 8123
    clickhouse_db = 'supervisor_dashboard'
    clickhouse_user = 'inkstone'
    clickhouse_password = 'U+jeKgAL'

    target_db = 'sv_engine'
    np_target_table = 'ds_227b41e8_5191_4f5a_96db_9309e1279d53'
    b5_target_table = 'ds_003b0c0a_fd07_4529_99e3_8828e58d15d6'
    
    mars_week_np_target_col = 'C_d5c89ad38b584188a0abfc1c3fadc34d'
    cal_channel_np_category_target_col = 'C_b4a11cff8bd74c76bc99e8e4d1533535'
    mars_region_np_target_col = 'C_1eb934abdffb4e828690db2f9d461a41'
    mars_province_np_target_col = 'C_aaa26934fa3f4d10b0b3221c0da8123c'

    mars_week_b5_target_col = 'C_dcbdbc2e91ea46cc9ba18150e70f4ddb'
    cal_channel_b5_category_target_col = 'C_467df952a3894fd4aa341d519d7c4d4c'
    mars_region_b5_target_col = 'C_179ed234d25d4c02a4d5c3edd4a6ef2b'
    mars_province_b5_target_col = 'C_0a264549fd164e2f8a7aaae75e1ccfaf'

    np_target_column_map =   {
        "C_47bb5de80dc24fb4a7c25094ad5be8da": "period",
        "C_d5c89ad38b584188a0abfc1c3fadc34d": "mars_week",
        "C_1eb934abdffb4e828690db2f9d461a41": "mars_region_name",
        "C_aaa26934fa3f4d10b0b3221c0da8123c": "mars_province_name",
        "C_b4a11cff8bd74c76bc99e8e4d1533535": "cal_channel_category",
        "C_2f1f827026a74ecfa28a888ad6e49db6": "cal_store_channel_category",
        "C_8f6a5651e70a415da0be978395c50dc5": "npd_sku",
        "C_c2bbed65ca2d4fa081961edc4076fc18": "sku_count_target",
        "C_5ebc79b051d142e18232d9d48dc0a57a": "acuracy_rate",
        "C_4a4f7bf5768b497fafa3e4b88cfd489a": "green_light_target"
    }

    b5_target_column_map = {
        "C_cfa0f1359ee645ad802b97a3ed46d7c8": "period",
        "C_dcbdbc2e91ea46cc9ba18150e70f4ddb": "mars_week",
        "C_179ed234d25d4c02a4d5c3edd4a6ef2b": "mars_region_name",
        "C_0a264549fd164e2f8a7aaae75e1ccfaf": "mars_province_name",
        "C_467df952a3894fd4aa341d519d7c4d4c": "cal_channel_category",
        "C_49ecd9b9957b40b7b6d3c1c7b07f04aa": "cal_store_channel_category",
        "C_1ea41336eaa34ba0bbeaee165c9ee0f5": "b5_sku",
        "C_42103e5985654eccb4d8d8542da23c9a": "sku_count_target",
        "C_8782e03156e34ec19251590d78f2399e": "acuracy_rate",
        "C_89884783473b43c28948bfe0e068a1e5": "green_light_target"
    }


elif env == 'prod':
    clickhouse_host = '10.216.3.89'
    clickhouse_port = 8123
    clickhouse_db = 'supervisor_dashboard'
    clickhouse_user = 'cl_defa'
    clickhouse_password = 'LC+ziNrP'

    target_db = 'sv_engine_data'
    np_target_table = 'ds_5a222fdd_4b75_4fcd_bc0b_f92ca056bb09'
    b5_target_table = 'ds_9f51980e_c5f0_4be8_b320_5dc6b8775dca'

    mars_week_np_target_col = 'C_44dee9cc75994491b575bed3bc1b02c4'
    cal_channel_np_category_target_col = 'C_e1688f5083754165a81b82c945a1eb16'
    mars_region_np_target_col = 'C_9afd604924f74d41bc5c317a30719924'
    mars_province_np_target_col = 'C_728e8b04b39e4ddba29135ace4d881c1'

    mars_week_b5_target_col = 'C_b8677723e1b1461fafbb11279594ef98'
    cal_channel_b5_category_target_col = 'C_235c5cda25ad4518a9f930b79bf70ac7'
    mars_region_b5_target_col = 'C_18f7ad01dd6843e1af6ba8a34eddf34e'
    mars_province_b5_target_col = 'C_aaab0c1afb2c4686be6310918c7af635'

    np_target_column_map =   {
        "C_51261e2788ba41b9889a6f6f9ba9c7c3": "period",
        "C_44dee9cc75994491b575bed3bc1b02c4": "mars_week",
        "C_9afd604924f74d41bc5c317a30719924": "mars_region_name",
        "C_728e8b04b39e4ddba29135ace4d881c1": "mars_province_name",
        "C_e1688f5083754165a81b82c945a1eb16": "cal_channel_category",
        "C_a4c1810f6d24410f9794759151c68716": "cal_store_channel_category",
        "C_96bdd7823ba24de9ab18684cf5264dd0": "npd_sku",
        "C_cd002c6f22f04e7bb20249600b118517": "sku_count_target",
        "C_1f0d528e976447be83de843d7ffeb20a": "acuracy_rate",
        "C_e173800c88d045639e8394aff66639b0": "green_light_target"
    }

    b5_target_column_map = {
        "C_c3a46897b2044196b09b05adc9eff3d8": "period",
        "C_b8677723e1b1461fafbb11279594ef98": "mars_week",
        "C_18f7ad01dd6843e1af6ba8a34eddf34e": "mars_region_name",
        "C_aaab0c1afb2c4686be6310918c7af635": "mars_province_name",
        "C_235c5cda25ad4518a9f930b79bf70ac7": "cal_channel_category",
        "C_d1844dfc3d6c40b59ac3b21e8bcb1990": "cal_store_channel_category",
        "C_bc3e1bb05f534e4590df4e72ec8fc5cd": "b5_sku",
        "C_0150b408edd34f79abd946edd70fea6a": "sku_count_target",
        "C_a8870b570db14aa8b8af8abecd81acb9": "acuracy_rate",
        "C_9333e2fd5c734e8ab75ea6c2dd6ebdab": "green_light_target"
    }
else:
    clickhouse_host = '???'
    clickhouse_port = 8123
    clickhouse_db = 'supervisor_dashboard'
    clickhouse_user = 'supervisor_dashboard_readonly'
    clickhouse_password = '???'


def trans_float(x):
    if '%' in str(x):
        return float(x.replace('%',''))/100
    elif pd.isna(x) or x==' ' or x=='':
        return None
    else:
        return x
def get_percent(x):
    try:
        if not pd.isna(x):
            return str(round(x*100,0))+"%"
        else:
            return ' '
    except:
        return ' '
    
def check_params(params):
    result = "( ( 1 = 2) Or ( 1 = 2) )" in params
    if result:
        return False
    else:
        return True
    
def get_mars_province_region(text):
    # 匹配所有地理字段类型
    pattern = r'(\w*?mars_(?:province|region|city|city_cluster)_name\b)\s*=\s*\'([^\']+)\''

    # 查找所有匹配项
    matches = re.findall(pattern, text)

    # 使用字典存储每个类型的所有值
    results = {
        "province": [],
        "region": [],
        "city": [],
        "city_cluster": []
    }

    # 分类并收集所有值
    for full_field, value in matches:
        if "mars_province_name" in full_field:
            results["province"].append((full_field, value))
        elif "mars_region_name" in full_field:
            results["region"].append((full_field, value))
        elif "mars_city_name" in full_field:
            results["city"].append((full_field, value))
        elif "mars_city_cluster_name" in full_field:
            results["city_cluster"].append((full_field, value))

    # 获取各类型的值列表
    provinces = [v[1] for v in results["province"]]
    regions = [v[1] for v in results["region"]]
    cities = [v[1] for v in results["city"]]
    clusters = [v[1] for v in results["city_cluster"]]

    return provinces, regions, cities, clusters


def calc_single(params):
    try:
        logger.info(f"接收参数 {params}")

        # 连接到ClickHouse服务器
        client = clickhouse_connect.get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            database=clickhouse_db,
            user=clickhouse_user,
            password=clickhouse_password,
            query_limit = 0
        )
        # 接收输入参数
        # params = {"period":"2025P07"}
        input_period = params.get('period',None)  # period
        params_sql = params['dataPermissionContextSQL']

        # 获取mars_week
        query_period_np = f"select max(mars_week) from store_np_sku_ttl where period in ('{input_period}')"
        df_mars_week = client.query_df(query_period_np)
        mars_week = df_mars_week.iloc[0,0]

        # 获取权限
        logger.info(params_sql)
        is_run = check_params(params_sql)
        if params_sql == '( ( 1 = 2)  Or  ( 1 = 2) )':
            logger.info("无权限")
            return genResultFromDf(pd.DataFrame())
        
        provinces, regions, cities, clusters = get_mars_province_region(params_sql)
        regions_target = []
        if provinces:
            quoted_provinces = ", ".join(f"'{i}'" for i in provinces)
            query_region = f"select DISTINCT mars_region_name from store_np_sku_ttl where mars_province_name in ({quoted_provinces})"
            df_region_name = client.query_df(query_region)
            region_name = df_region_name['mars_region_name'].to_list()
            regions_target.extend(region_name)

        if cities:
            quoted_cities = ", ".join(f"'{i}'" for i in cities)
            query_province = f"select DISTINCT mars_province_name from store_np_sku_ttl where mars_city_name in ({quoted_cities})"
            df_province_name = client.query_df(query_province)
            province_name = df_province_name['mars_province_name'].to_list()
            provinces.extend(province_name)


            quoted_provinces = ", ".join(f"'{i}'" for i in provinces)
            query_region = f"select DISTINCT mars_region_name from store_np_sku_ttl where mars_province_name in ({quoted_provinces})"
            df_region_name = client.query_df(query_region)
            region_name = df_region_name['mars_region_name'].to_list()
            regions_target.extend(region_name)

        if clusters:
            quoted_clusters = ", ".join(f"'{i}'" for i in clusters)
            query_province = f"select DISTINCT mars_province_name from store_np_sku_ttl where mars_city_cluster_name in ({quoted_clusters})"
            df_province_name = client.query_df(query_province)
            province_name = df_province_name['mars_province_name'].to_list()
            provinces.extend(province_name)

            quoted_provinces = ", ".join(f"'{i}'" for i in provinces)
            query_region = f"select DISTINCT mars_region_name from store_np_sku_ttl where mars_province_name in ({quoted_provinces})"
            df_region_name = client.query_df(query_region)
            region_name = df_region_name['mars_region_name'].to_list()
            regions_target.extend(region_name)

        if not regions and not provinces and not cities and not clusters:
            extra_ttl_condition = ""
            extra_np_target_condition = ""
            extra_b5_target_condition = ""
        else:
            regions_target = list(set(regions_target))
            provinces = list(set(provinces))
            quoted_regions_target = ", ".join(f"'{i}'" for i in regions_target)
            quoted_provinces = ", ".join(f"'{i}'" for i in provinces)

            if regions:
                regions = list(set(regions))
                quoted_regions = ", ".join(f"'{i}'" for i in regions)
                if not provinces:
                    extra_ttl_condition = f" AND mars_region_name in ({quoted_regions}) "
                    extra_np_target_condition = f" AND ({mars_region_np_target_col} in ({quoted_regions}) OR ({mars_region_np_target_col} IS NULL)) "
                    extra_b5_target_condition = f" AND ({mars_region_b5_target_col} in ({quoted_regions}) OR ({mars_region_b5_target_col} IS NULL)) "
                else:
                    extra_ttl_condition = f" AND ( mars_region_name in ({quoted_regions}) OR mars_province_name in ({quoted_provinces}) )"
                    extra_np_target_condition = f" AND ({mars_province_np_target_col} in ({quoted_provinces}) OR ({mars_region_np_target_col} in ({quoted_regions_target}) AND {mars_province_np_target_col} IS NULL) OR ({mars_region_np_target_col} in ({quoted_regions})) OR ({mars_region_np_target_col} IS NULL AND {mars_province_np_target_col} IS NULL)) "
                    extra_b5_target_condition = f" AND ({mars_province_b5_target_col} in ({quoted_provinces}) OR ({mars_region_b5_target_col} in ({quoted_regions_target}) AND {mars_province_b5_target_col} IS NULL) OR ({mars_region_b5_target_col} in ({quoted_regions})) OR ({mars_region_b5_target_col} IS NULL AND {mars_province_b5_target_col} IS NULL)) "
            else:
                extra_ttl_condition = f" AND mars_province_name in ({quoted_provinces}) "
                extra_np_target_condition = f" AND ({mars_province_np_target_col} in ({quoted_provinces}) OR ({mars_region_np_target_col} in ({quoted_regions_target}) AND {mars_province_np_target_col} IS NULL) OR ({mars_region_np_target_col} IS NULL AND {mars_province_np_target_col} IS NULL)) "
                extra_b5_target_condition = f" AND ({mars_province_b5_target_col} in ({quoted_provinces}) OR ({mars_region_b5_target_col} in ({quoted_regions_target}) AND {mars_province_b5_target_col} IS NULL) OR ({mars_region_b5_target_col} IS NULL AND {mars_province_b5_target_col} IS NULL)) "

        # 读取NP数据
        query = f"""
        select period, mars_week, npd_sku, mars_region_name, mars_province_name, sum(npd_sku_count) as npd_sku_count
        from store_np_sku_ttl	 
        where mars_week = '{mars_week}' 
        and rtm_channel_name <> 'NKA'
        and table_type = 'ttl' {extra_ttl_condition}
        group by period, mars_week, npd_sku, mars_region_name, mars_province_name
        """
        logger.info(query)
        df_np = client.query_df(query)

        # 读取NP外部数据
        query_target = f"""
        select *
        from {target_db}.{np_target_table}	 
        where {mars_week_np_target_col} = '{mars_week}' 
        and {cal_channel_np_category_target_col} = 'TTL' {extra_np_target_condition}
        """
        logger.info(query_target)
        df_np_target = client.query_df(query_target)

        df_np_province = df_np.copy()
        df_np_region = df_np.groupby(['period', 'mars_week', 'npd_sku', 'mars_region_name']).agg(npd_sku_count=('npd_sku_count', 'sum')).reset_index()
        df_np_total = df_np.groupby(['period', 'mars_week', 'npd_sku']).agg(npd_sku_count=('npd_sku_count', 'sum')).reset_index()

        df_np_target = df_np_target.rename(columns=np_target_column_map)
        df_np_target_province = df_np_target[(~df_np_target['mars_province_name'].isna()) & (~df_np_target['mars_region_name'].isna())].reset_index(drop=True)
        df_np_target_region = df_np_target[(df_np_target['mars_province_name'].isna()) & (~df_np_target['mars_region_name'].isna())].reset_index(drop=True)
        df_np_target_total = df_np_target[(df_np_target['mars_province_name'].isna()) & (df_np_target['mars_region_name'].isna())].reset_index(drop=True)

        # 计算省份目标达成率
        target_join_colums = ['period','mars_week','npd_sku','mars_region_name','mars_province_name','sku_count_target','acuracy_rate']
        target_on_column = ['period', 'mars_week', 'npd_sku', 'mars_region_name','mars_province_name']
        df_np_province = df_np_province.merge(df_np_target_province[target_join_colums], on=target_on_column, how='left')
        columns_to_convert = ['npd_sku_count', 'sku_count_target', 'acuracy_rate']
        for col in columns_to_convert:
            df_np_province[col] = df_np_province[col].astype(float)
        adjusted_acuracy_rate = df_np_province['acuracy_rate'].fillna(1).copy()
        df_np_province['achievement_rate'] = df_np_province['npd_sku_count'] / df_np_province['sku_count_target']*adjusted_acuracy_rate
        df_np_province['achievement_rate'] = df_np_province['achievement_rate'].clip(upper=1.1)
        df_np_province['achievement_rate'] = df_np_province['achievement_rate'].round(4)

        # 计算区目标达成率
        target_join_colums.remove('mars_province_name')
        target_on_column.remove('mars_province_name')
        df_np_region = df_np_region.merge(df_np_target_region[target_join_colums], on=target_on_column, how='left')
        columns_to_convert = ['npd_sku_count', 'sku_count_target', 'acuracy_rate']
        for col in columns_to_convert:
            df_np_region[col] = df_np_region[col].astype(float)
        adjusted_acuracy_rate = df_np_region['acuracy_rate'].fillna(1).copy()
        df_np_region['achievement_rate'] = df_np_region['npd_sku_count'] / df_np_region['sku_count_target']*adjusted_acuracy_rate
        df_np_region['achievement_rate'] = df_np_region['achievement_rate'].clip(upper=1.1)
        df_np_region['achievement_rate'] = df_np_region['achievement_rate'].round(4)

        # 计算总目标达成率
        target_join_colums.remove('mars_region_name')
        target_on_column.remove('mars_region_name')
        df_np_total = df_np_total.merge(df_np_target_total[target_join_colums], on=target_on_column, how='left')
        columns_to_convert = ['npd_sku_count', 'sku_count_target', 'acuracy_rate']
        for col in columns_to_convert:
            df_np_total[col] = df_np_total[col].astype(float)
        adjusted_acuracy_rate = df_np_total['acuracy_rate'].fillna(1).copy()
        df_np_total['achievement_rate'] = df_np_total['npd_sku_count'] / df_np_total['sku_count_target']*adjusted_acuracy_rate
        df_np_total['achievement_rate'] = df_np_total['achievement_rate'].clip(upper=1.1)
        df_np_total['achievement_rate'] = df_np_total['achievement_rate'].round(4)


        # 读取B5数据
        query = f"""
        select period, mars_week, b5_sku, mars_region_name, mars_province_name, sum(b5_sku_count) as b5_sku_count
        from store_b5_sku_ttl	 
        where mars_week = '{mars_week}'
        and rtm_channel_name <> 'NKA'
        and table_type = 'ttl' {extra_ttl_condition}
        group by period, mars_week, b5_sku, mars_region_name, mars_province_name
        """
        logger.info(query)
        df_b5 = client.query_df(query)

        # 读取B5外部数据
        query_target = f"""
        select *
        from {target_db}.{b5_target_table}	 
        where {mars_week_b5_target_col} = '{mars_week}' 
        and {cal_channel_b5_category_target_col} = 'TTL' {extra_b5_target_condition}
        """
        logger.info(query_target)
        df_b5_target = client.query_df(query_target)

        df_b5_province = df_b5.copy()
        df_b5_region = df_b5.groupby(['period', 'mars_week', 'b5_sku', 'mars_region_name']).agg(b5_sku_count=('b5_sku_count', 'sum')).reset_index()
        df_b5_total = df_b5.groupby(['period', 'mars_week', 'b5_sku']).agg(b5_sku_count=('b5_sku_count', 'sum')).reset_index()

        df_b5_target = df_b5_target.rename(columns=b5_target_column_map)
        df_b5_target_province = df_b5_target[(~df_b5_target['mars_province_name'].isna()) & (~df_b5_target['mars_region_name'].isna())].reset_index(drop=True)
        df_b5_target_region = df_b5_target[(df_b5_target['mars_province_name'].isna()) & (~df_b5_target['mars_region_name'].isna())].reset_index(drop=True)
        df_b5_target_total = df_b5_target[(df_b5_target['mars_province_name'].isna()) & (df_b5_target['mars_region_name'].isna())].reset_index(drop=True)

        # 计算省份目标达成率
        target_join_colums = ['period','mars_week','b5_sku','mars_region_name','mars_province_name','sku_count_target','acuracy_rate']
        target_on_column = ['period', 'mars_week', 'b5_sku', 'mars_region_name','mars_province_name']
        df_b5_province = df_b5_province.merge(df_b5_target_province[target_join_colums], on=target_on_column, how='left')
        columns_to_convert = ['b5_sku_count', 'sku_count_target', 'acuracy_rate']
        for col in columns_to_convert:
            df_b5_province[col] = df_b5_province[col].astype(float)
        adjusted_acuracy_rate = df_b5_province['acuracy_rate'].fillna(1).copy()
        df_b5_province['achievement_rate'] = df_b5_province['b5_sku_count'] / df_b5_province['sku_count_target']*adjusted_acuracy_rate
        df_b5_province['achievement_rate'] = df_b5_province['achievement_rate'].clip(upper=1.1)
        df_b5_province['achievement_rate'] = df_b5_province['achievement_rate'].round(4)

        # 计算区目标达成率
        target_join_colums.remove('mars_province_name')
        target_on_column.remove('mars_province_name')
        df_b5_region = df_b5_region.merge(df_b5_target_region[target_join_colums], on=target_on_column, how='left')
        columns_to_convert = ['b5_sku_count', 'sku_count_target', 'acuracy_rate']
        for col in columns_to_convert:
            df_b5_region[col] = df_b5_region[col].astype(float)
        adjusted_acuracy_rate = df_b5_region['acuracy_rate'].fillna(1).copy()
        df_b5_region['achievement_rate'] = df_b5_region['b5_sku_count'] / df_b5_region['sku_count_target']*adjusted_acuracy_rate
        df_b5_region['achievement_rate'] = df_b5_region['achievement_rate'].clip(upper=1.1)
        df_b5_region['achievement_rate'] = df_b5_region['achievement_rate'].round(4)

        # 计算总目标达成率
        target_join_colums.remove('mars_region_name')
        target_on_column.remove('mars_region_name')
        df_b5_total = df_b5_total.merge(df_b5_target_total[target_join_colums], on=target_on_column, how='left')
        columns_to_convert = ['b5_sku_count', 'sku_count_target', 'acuracy_rate']
        for col in columns_to_convert:
            df_b5_total[col] = df_b5_total[col].astype(float)
        adjusted_acuracy_rate = df_b5_total['acuracy_rate'].fillna(1).copy()
        df_b5_total['achievement_rate'] = df_b5_total['b5_sku_count'] / df_b5_total['sku_count_target']*adjusted_acuracy_rate
        df_b5_total['achievement_rate'] = df_b5_total['achievement_rate'].clip(upper=1.1)
        df_b5_total['achievement_rate'] = df_b5_total['achievement_rate'].round(4) 

        # 计算平均值

        keep_cols = ['period','mars_region_name','mars_province_name','achievement_rate','sku_count_target']
        df_province = pd.concat([df_np_province[keep_cols],df_b5_province[keep_cols]], ignore_index=True)
        df_denominator = df_province.groupby(['period','mars_region_name', 'mars_province_name'])['sku_count_target'].count().reset_index()
        df_province = df_province.groupby(['period','mars_region_name','mars_province_name']).agg(achievement_rate=('achievement_rate', 'sum')).reset_index()
        df_province = pd.merge(df_province, df_denominator, on=['period','mars_region_name','mars_province_name'], how='left')
        df_province['achievement_rate'] = (df_province['achievement_rate'] / df_province['sku_count_target']).round(4)

        keep_cols = ['period','mars_region_name','achievement_rate','sku_count_target']
        df_region = pd.concat([df_np_region[keep_cols],df_b5_region[keep_cols]], ignore_index=True)
        df_denominator = df_region.groupby(['period','mars_region_name'])['sku_count_target'].count().reset_index()
        df_region = df_region.groupby(['period','mars_region_name']).agg(achievement_rate=('achievement_rate', 'sum')).reset_index()
        df_region = pd.merge(df_region, df_denominator, on=['period','mars_region_name'], how='left')
        df_region['achievement_rate'] = (df_region['achievement_rate'] / df_region['sku_count_target']).round(4)

        keep_cols = ['period','achievement_rate','sku_count_target']
        df_total = pd.concat([df_np_total[keep_cols],df_b5_total[keep_cols]], ignore_index=True)
        df_denominator = df_total.groupby(['period'])['sku_count_target'].count().reset_index()
        df_total = df_total.groupby(['period']).agg(achievement_rate=('achievement_rate', 'sum')).reset_index()
        df_total = pd.merge(df_total, df_denominator, on=['period'], how='left')
        df_total['achievement_rate'] = (df_total['achievement_rate'] / df_total['sku_count_target']).round(4)

        df_result = pd.concat([df_province, df_region, df_total], ignore_index=True)
        df_result.drop(columns=['sku_count_target'], inplace=True)

        return genResultFromDf(df_result)
    except Exception as e:
        # 打印详细报错信息
        logger.error(traceback.print_exc())
    finally:
        # 关闭连接
        client.close()

def genResultFromDf(df_r: pd.DataFrame):
    # 构造输出
    data = []
    for v in list(df_r.values):
        r = []
        for cell in v:
            if type(cell) is pd._libs.missing.NAType or pd.isna(cell):
                cell = None
            r.append(cell)
        data.append(r)
    r = {'fields': [{'name': c, 'type': 'string'} for c in df_r.columns],
         'data': data}
    logger.info('df_r:\n{}', df_r.head())
    return r
if __name__ == "__main__":

    # r = calc_single({'platform': 'TMALL', 'date_range': ['1677427200000', '1677945600000'], 'mw_category_name': 'CHO'})
    # print(r)

    params_path = sys.argv[1]
    with open(params_path, 'rb') as f:
        execute_params = json.load(f)
        result = None
        config_path = execute_params.get('config_path')
        algorithm_io_mode = execute_params.get('algorithm_io_mode')
        if algorithm_io_mode == 'SINGLE':
            result = calc_single(execute_params.get('params', {}))
        # print(json.dumps(result))
    with open(params_path, 'w') as f:
        f.write(json.dumps(result))
