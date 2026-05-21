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
    np_target_table = 'ds_05750a4a_c137_4588_ac8c_60e14dd4cb19'
    b5_target_table = 'ds_c22da242_d31b_47a3_802a_6442b59a0b1f'

    mars_week_np_target_col = 'C_861004a26be94f5cbe5debc07a827e89'
    cal_channel_np_category_target_col = 'C_6a552727550844d085885c9ea4484372'
    cal_ncd_np_target_col = 'C_663740b5f99548078b2c71ce0688a555'

    mars_week_b5_target_col = 'C_9e06c721f2654eaa8373203bf48a5b88'
    cal_channel_b5_category_target_col = 'C_ddf3e4f20e97495f89dfd541dff3681c'
    cal_ncd_b5_target_col = 'C_e815d239774440bba37f15f98d6591aa'
    np_target_column_map =   {
        "C_76df3cc57579435f83713b7b29ab5a86": "period",
        "C_861004a26be94f5cbe5debc07a827e89": "mars_week",
        "C_35b7b168a1374681a4f6abe742ae8994": "cal_ncd_category",
        "C_663740b5f99548078b2c71ce0688a555": "cal_ncd",
        "C_6a552727550844d085885c9ea4484372": "cal_channel_category",
        "C_b2f870ebe77941bd8772d695f5d0787a": "cal_store_channel_category",
        "C_783595b7d27a414f8bb89f36dcaaec2c": "npd_sku",
        "C_ca1c2798d3ab4682b1f978c49ae5793f": "sku_count_target",
        "C_8af0d420a1964db280488de7f468c999": "acuracy_rate",
        "C_f9a8bff94c7347089a1695b58238fb06": "green_light_target",
        "C_767d3dddb2f94cb8808a9c7882ac7b19": "filter_out"
    }

    b5_target_column_map = {
        "C_38bc54725fd341218ff5c84ebf281afd": "period",
        "C_9e06c721f2654eaa8373203bf48a5b88": "mars_week",
        "C_279118472bc14ae58a10913026fd38e5": "cal_ncd_category",
        "C_e815d239774440bba37f15f98d6591aa": "cal_ncd",
        "C_ddf3e4f20e97495f89dfd541dff3681c": "cal_channel_category",
        "C_78fb02959d1f4f7785a61e356e0e6de6": "cal_store_channel_category",
        "C_54018814ca1a4f7ea235425090f31885": "b5_sku",
        "C_6528e246d4f0468997ad40350025b9e4": "sku_count_target",
        "C_dfd92627cbe5496887eaf31cc58e7c38": "acuracy_rate",
        "C_9d0fe928f244470683410cfc0bff64db": "green_light_target",
        "C_66183aa381cf474db9d215f169e4fa00": "filter_out"
    }


elif env == 'prod':
    clickhouse_host = '10.216.3.89'
    clickhouse_port = 8123
    clickhouse_db = 'supervisor_dashboard'
    clickhouse_user = 'cl_defa'
    clickhouse_password = 'LC+ziNrP'

    target_db = 'sv_engine_data'
    np_target_table = 'ds_ac7e504e_b2c0_4cc9_b97d_3876fc349b67'
    b5_target_table = 'ds_3c3fb161_50ab_4edd_a231_d89e1e9689a7'

    mars_week_np_target_col = 'C_8f456b54d76741cdaeefb95a322e4d3a'
    cal_channel_np_category_target_col = 'C_8005b903afc14b46a1f553e25f662843'
    cal_ncd_np_target_col = 'C_63649111a0054c23b38887620063a52c'

    mars_week_b5_target_col = 'C_c56d8139e2a54a9db543d41b04d5a74f'
    cal_channel_b5_category_target_col = 'C_f98acaf5039d4845b5e11ab61cf0d5eb'
    cal_ncd_b5_target_col = 'C_dafcbd64a69c4dffae7269ee0b966ef3'

    np_target_column_map =   {
        "C_c1efd25bf6cb41bd9522db290f6875c1": "period",
        "C_8f456b54d76741cdaeefb95a322e4d3a": "mars_week",
        "C_49264a475b82402b9f9972a84a88850b": "cal_ncd_category",
        "C_63649111a0054c23b38887620063a52c": "cal_ncd",
        "C_8005b903afc14b46a1f553e25f662843": "cal_channel_category",
        "C_86633cd20a2a452e9c8c1aa1bd6c6be5": "cal_store_channel_category",
        "C_3edc5780d2d147b4b063faa2c4ab06db": "npd_sku",
        "C_161d3ccd49794583872f4a810dc43271": "sku_count_target",
        "C_36fdf49453fe4b62948a4e79747b4cee": "acuracy_rate",
        "C_7b0efcd58ced41aabd5909356a121ef7": "green_light_target",
        # TODO: 待补充 prod 环境 np_target_table【剔除基础执行考核】列编码
        "TODO_NP_FILTER_OUT_PROD": "filter_out"
    }

    b5_target_column_map = {
        "C_e00d041bed624732bd861f619dece2a5": "period",
        "C_c56d8139e2a54a9db543d41b04d5a74f": "mars_week",
        "C_364f34fcb1c7488b962be21124dd8d33": "cal_ncd_category",
        "C_dafcbd64a69c4dffae7269ee0b966ef3": "cal_ncd",
        "C_f98acaf5039d4845b5e11ab61cf0d5eb": "cal_channel_category",
        "C_b179fc23e7874a1f9b1da2ca5da478c9": "cal_store_channel_category",
        "C_85d3d9fb18864f0eb0ac42be41aaa846": "b5_sku",
        "C_47ffd259a15a4621a42dcb4b958b60a7": "sku_count_target",
        "C_d556e1ff85d647c6bd21ffd4e48e5023": "acuracy_rate",
        "C_7dd60f998784468a8d1e9a2808650c71": "green_light_target",
        # TODO: 待补充 prod 环境 b5_target_table【剔除基础执行考核】列编码
        "TODO_B5_FILTER_OUT_PROD": "filter_out"
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

def _has_value(series):
    return series.notna() & (series.astype(str).str.strip() != '')

def _normalize_filter_key_columns(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].where(df[col].notna(), np.nan)
            str_mask = df[col].map(lambda x: isinstance(x, str))
            df.loc[str_mask, col] = df.loc[str_mask, col].map(lambda x: x.strip())
            df.loc[df[col] == '', col] = np.nan
    return df

def build_filter_out_rules(df, *, sku_col):
    required_cols = {sku_col, 'cal_ncd_category', 'cal_ncd', 'filter_out'}
    if df.empty or not required_cols.issubset(df.columns):
        return pd.DataFrame(columns=[sku_col, 'cal_ncd_category', 'cal_ncd'])

    filter_out_num = pd.to_numeric(df['filter_out'], errors='coerce')
    rules = df.loc[filter_out_num == 1, [sku_col, 'cal_ncd_category', 'cal_ncd']].copy()
    rules = _normalize_filter_key_columns(rules, [sku_col, 'cal_ncd_category', 'cal_ncd'])
    rules = rules[_has_value(rules[sku_col]) & _has_value(rules['cal_ncd_category'])]
    return rules.drop_duplicates().reset_index(drop=True)

def apply_filter_out_rules(df, rules, *, sku_col):
    if df.empty or rules.empty:
        return df
    if not {sku_col, 'cal_ncd_category', 'cal_ncd'}.issubset(df.columns):
        return df

    filtered_df = _normalize_filter_key_columns(df.copy(), [sku_col, 'cal_ncd_category', 'cal_ncd'])
    mask = pd.Series(True, index=filtered_df.index)
    for _, rule in rules.iterrows():
        sku_mask = filtered_df[sku_col] == rule[sku_col]
        if pd.notna(rule['cal_ncd']):
            mask &= ~(sku_mask & (filtered_df['cal_ncd'] == rule['cal_ncd']))
        else:
            mask &= ~(sku_mask & (filtered_df['cal_ncd_category'] == rule['cal_ncd_category']))
    return filtered_df[mask].reset_index(drop=True)

def rebuild_affected_target_rollups(df, rules, *, sku_col):
    if df.empty or rules.empty:
        return df
    required_cols = {sku_col, 'cal_ncd_category', 'cal_ncd', 'sku_count_target'}
    if not required_cols.issubset(df.columns):
        return df

    result = _normalize_filter_key_columns(df.copy(), [sku_col, 'cal_ncd_category', 'cal_ncd'])
    province_rules = rules[rules['cal_ncd'].notna()].drop_duplicates([sku_col, 'cal_ncd_category'])

    for _, rule in province_rules.iterrows():
        sku_value = rule[sku_col]
        category_value = rule['cal_ncd_category']
        detail_mask = (
            (result[sku_col] == sku_value)
            & (result['cal_ncd_category'] == category_value)
            & _has_value(result['cal_ncd'])
        )
        rollup_mask = (
            (result[sku_col] == sku_value)
            & (result['cal_ncd_category'] == category_value)
            & (~_has_value(result['cal_ncd']))
        )
        target_sum = pd.to_numeric(result.loc[detail_mask, 'sku_count_target'], errors='coerce').sum(min_count=1)
        if pd.isna(target_sum):
            result = result.loc[~rollup_mask].copy()
        elif rollup_mask.any():
            result.loc[rollup_mask, 'sku_count_target'] = target_sum

    for sku_value in rules[sku_col].dropna().unique():
        category_mask = (
            (result[sku_col] == sku_value)
            & _has_value(result['cal_ncd_category'])
            & (~_has_value(result['cal_ncd']))
        )
        total_mask = (
            (result[sku_col] == sku_value)
            & (~_has_value(result['cal_ncd_category']))
            & (~_has_value(result['cal_ncd']))
        )
        target_sum = pd.to_numeric(result.loc[category_mask, 'sku_count_target'], errors='coerce').sum(min_count=1)
        if pd.isna(target_sum):
            result = result.loc[~total_mask].copy()
        elif total_mask.any():
            result.loc[total_mask, 'sku_count_target'] = target_sum
    return result.reset_index(drop=True)

def filter_target_rows(df, *, require_sku_target, sku_col=None, filter_out_rules=None):
    # 剔除外部目标数据中不参与匹配的行：
    # - filter_out == 1 时，按 sku + cal_ncd 或 sku + cal_ncd_category 生成剔除规则
    # - require_sku_target=True 时，sku_count_target 为空也整行排除
    if df.empty:
        return df

    if sku_col and sku_col in df.columns:
        if filter_out_rules is None:
            filter_out_rules = build_filter_out_rules(df, sku_col=sku_col)
        filtered_df = apply_filter_out_rules(df, filter_out_rules, sku_col=sku_col)
        filtered_df = rebuild_affected_target_rollups(filtered_df, filter_out_rules, sku_col=sku_col)
    else:
        filtered_df = df.copy()
        if 'filter_out' in filtered_df.columns:
            # 列存储为 Decimal，需先转数值再比较，否则 Decimal('1.0000') != '1'
            filter_out_num = pd.to_numeric(filtered_df['filter_out'], errors='coerce')
            filtered_df = filtered_df[filter_out_num != 1]

    if require_sku_target and 'sku_count_target' in df.columns:
        filtered_df = filtered_df[filtered_df['sku_count_target'].notna()]
    return filtered_df.reset_index(drop=True)
    


# ---------------- DEBUG LOG HELPERS (Python 3.7 compatible) ----------------
DEBUG_SAMPLE_ROWS = int(os.environ.get('DEBUG_SAMPLE_ROWS', '20'))

def _debug_cell_value(x):
    """Convert values to log-safe primitives. Compatible with Python 3.7 / pandas older versions."""
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    try:
        if isinstance(x, np.generic):
            return x.item()
    except Exception:
        pass
    if isinstance(x, Decimal):
        return str(x)
    try:
        if isinstance(x, (datetime.datetime, datetime.date)):
            return x.isoformat()
    except Exception:
        pass
    return x

def _debug_records(df, cols=None, limit=None):
    """Return a small list[dict] sample. Avoid DataFrame.to_string for compatibility and log size."""
    if limit is None:
        limit = DEBUG_SAMPLE_ROWS
    try:
        if df is None:
            return []
        if cols:
            real_cols = [c for c in cols if c in df.columns]
            sample = df.loc[:, real_cols].head(limit).copy()
        else:
            sample = df.head(limit).copy()
        for c in sample.columns:
            sample[c] = sample[c].map(_debug_cell_value)
        return sample.to_dict('records')
    except Exception as e:
        return [{'debug_error': repr(e)}]

def _debug_count_notna(df, col):
    try:
        if df is None or col not in df.columns:
            return None
        return int(df[col].notna().sum())
    except Exception:
        return None

def _debug_nunique(df, col):
    try:
        if df is None or col not in df.columns:
            return None
        return int(df[col].nunique(dropna=True))
    except Exception:
        return None

def _debug_sum_numeric(df, col):
    try:
        if df is None or col not in df.columns:
            return None
        v = pd.to_numeric(df[col], errors='coerce').sum()
        if pd.isna(v):
            return None
        return float(v)
    except Exception:
        return None

def _debug_df_basic(name, df, cols=None, sample_rows=None):
    """Small, safe DataFrame debug log."""
    try:
        if df is None:
            logger.info('[DEBUG] {}: None', name)
            return
        logger.info('[DEBUG] {} shape={} columns={}', name, tuple(df.shape), list(df.columns))
        if cols:
            real_cols = [c for c in cols if c in df.columns]
            null_counts = {}
            dtypes = {}
            for c in real_cols:
                try:
                    null_counts[c] = int(df[c].isna().sum())
                except Exception:
                    null_counts[c] = 'ERR'
                try:
                    dtypes[c] = str(df[c].dtype)
                except Exception:
                    dtypes[c] = 'ERR'
            logger.info('[DEBUG] {} selected_dtypes={}', name, dtypes)
            logger.info('[DEBUG] {} selected_null_counts={}', name, null_counts)
            logger.info('[DEBUG] {} sample_records={}', name, _debug_records(df, real_cols, sample_rows))
    except Exception as e:
        logger.info('[DEBUG] {} log_failed={}', name, repr(e))

def _debug_filter_out_distribution(name, df):
    try:
        if df is None or 'filter_out' not in df.columns:
            logger.info('[DEBUG] {} filter_out_dist=NO_COLUMN', name)
            return
        s = pd.to_numeric(df['filter_out'], errors='coerce')
        dist = s.value_counts(dropna=False).head(20).to_dict()
        safe_dist = {}
        for k, v in dist.items():
            safe_dist[str(_debug_cell_value(k))] = int(v)
        logger.info('[DEBUG] {} filter_out_dist={}', name, safe_dist)
    except Exception as e:
        logger.info('[DEBUG] {} filter_out_dist_failed={}', name, repr(e))

def _debug_total_target(name, df, sku_col):
    cols = ['period', 'mars_week', sku_col, 'cal_ncd_category', 'cal_ncd', 'sku_count_target', 'acuracy_rate', 'filter_out']
    try:
        if df is None or df.empty:
            logger.info('[DEBUG] {} total_target empty_or_none shape={}', name, None if df is None else tuple(df.shape))
            return
        if 'cal_ncd' in df.columns and 'cal_ncd_category' in df.columns:
            total_df = df[df['cal_ncd'].isna() & df['cal_ncd_category'].isna()].copy()
        else:
            total_df = df.copy()
        logger.info('[DEBUG] {} total_target_shape={}', name, tuple(total_df.shape))
        logger.info('[DEBUG] {} total_target_sku_nunique={}', name, _debug_nunique(total_df, sku_col))
        logger.info('[DEBUG] {} total_target_target_notna={}', name, _debug_count_notna(total_df, 'sku_count_target'))
        logger.info('[DEBUG] {} total_target_target_sum={}', name, _debug_sum_numeric(total_df, 'sku_count_target'))
        logger.info('[DEBUG] {} total_target_sample={}', name, _debug_records(total_df, cols, DEBUG_SAMPLE_ROWS))
    except Exception as e:
        logger.info('[DEBUG] {} total_target_log_failed={}', name, repr(e))

def _debug_total_calc(name, df, sku_col, count_col):
    cols = ['period', 'mars_week', sku_col, count_col, 'sku_count_target', 'acuracy_rate', 'achievement_rate']
    try:
        logger.info('[DEBUG] {} shape={}', name, tuple(df.shape) if df is not None else None)
        logger.info('[DEBUG] {} sku_nunique={}', name, _debug_nunique(df, sku_col))
        logger.info('[DEBUG] {} target_notna={}', name, _debug_count_notna(df, 'sku_count_target'))
        logger.info('[DEBUG] {} achievement_sum={}', name, _debug_sum_numeric(df, 'achievement_rate'))
        logger.info('[DEBUG] {} sample={}', name, _debug_records(df, cols, DEBUG_SAMPLE_ROWS))
    except Exception as e:
        logger.info('[DEBUG] {} total_calc_log_failed={}', name, repr(e))
# ---------------------------------------------------------------------------

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
        logger.info('[DEBUG] python_version={}', sys.version)
        logger.info('[DEBUG] pandas_version={}', pd.__version__)
        logger.info('[DEBUG] numpy_version={}', np.__version__)
        logger.info('[DEBUG] actual_params_period={}', input_period)
        logger.info('[DEBUG] actual_params_sql=[{}]', params_sql)

        # 获取mars_week
        query_period_np = f"select max(mars_week) from store_np_sku_ttl where period in ('{input_period}')"
        df_mars_week = client.query_df(query_period_np)
        mars_week = df_mars_week.iloc[0,0]
        logger.info('[DEBUG] query_period_np={}', query_period_np)
        _debug_df_basic('df_mars_week', df_mars_week)
        logger.info('[DEBUG] mars_week={}', mars_week)

        # 获取权限
        logger.info(params_sql)
        is_run = check_params(params_sql)
        if params_sql == '( ( 1 = 2)  Or  ( 1 = 2) )':
            logger.info("无权限")
            return genResultFromDf(pd.DataFrame())
        
        provinces, regions, cities, clusters = get_mars_province_region(params_sql)
        regions_target = []

        if regions:
            quoted_regions = ", ".join(f"'{i}'" for i in regions)
            query_ncd = f"select DISTINCT cal_ncd from store_np_sku_ttl where mars_region_name in ({quoted_regions})"
            df_ncd_name = client.query_df(query_ncd)
            ncd_name = df_ncd_name['cal_ncd'].to_list()
            regions_target.extend(ncd_name)

        if provinces:
            quoted_provinces = ", ".join(f"'{i}'" for i in provinces)
            query_region = f"select DISTINCT cal_ncd from store_np_sku_ttl where mars_province_name in ({quoted_provinces})"
            df_region_name = client.query_df(query_region)
            region_name = df_region_name['cal_ncd'].to_list()
            regions_target.extend(region_name)

        if cities:
            quoted_cities = ", ".join(f"'{i}'" for i in cities)
            query_province = f"select DISTINCT mars_province_name from store_np_sku_ttl where mars_city_name in ({quoted_cities})"
            df_province_name = client.query_df(query_province)
            province_name = df_province_name['mars_province_name'].to_list()
            provinces.extend(province_name)


            quoted_provinces = ", ".join(f"'{i}'" for i in provinces)
            query_region = f"select DISTINCT cal_ncd from store_np_sku_ttl where mars_province_name in ({quoted_provinces})"
            df_region_name = client.query_df(query_region)
            region_name = df_region_name['cal_ncd'].to_list()
            regions_target.extend(region_name)

        if clusters:
            quoted_clusters = ", ".join(f"'{i}'" for i in clusters)
            query_province = f"select DISTINCT mars_province_name from store_np_sku_ttl where mars_city_cluster_name in ({quoted_clusters})"
            df_province_name = client.query_df(query_province)
            province_name = df_province_name['mars_province_name'].to_list()
            provinces.extend(province_name)

            quoted_provinces = ", ".join(f"'{i}'" for i in provinces)
            query_region = f"select DISTINCT cal_ncd from store_np_sku_ttl where mars_province_name in ({quoted_provinces})"
            df_region_name = client.query_df(query_region)
            region_name = df_region_name['cal_ncd'].to_list()
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

            regions = list(set(regions))
            quoted_regions = ", ".join(f"'{i}'" for i in regions)
            extra_ttl_condition = f" AND cal_ncd in ({quoted_regions_target})"
            extra_np_target_condition = f" AND (({cal_ncd_np_target_col} in ({quoted_regions_target})) OR ({cal_ncd_np_target_col} IS NULL)) "
            extra_b5_target_condition = f" AND ( ({cal_ncd_b5_target_col} in ({quoted_regions_target})) OR ({cal_ncd_b5_target_col} IS NULL)) "
        

        # 读取NP数据
        query = f"""
        select period, mars_week, npd_sku, cal_ncd_category, cal_ncd, sum(npd_sku_count) as npd_sku_count
        from store_np_sku_ttl	 
        where mars_week = '{mars_week}' 
        and table_type = 'ttl' {extra_ttl_condition}
        group by period, mars_week, npd_sku, cal_ncd_category, cal_ncd
        """
        logger.info(query)
        df_np = client.query_df(query)
        _debug_df_basic('df_np_raw', df_np, ['period', 'mars_week', 'npd_sku', 'cal_ncd_category', 'cal_ncd', 'npd_sku_count'])
        logger.info('[DEBUG] df_np_raw npd_sku_nunique={}', _debug_nunique(df_np, 'npd_sku'))
        logger.info('[DEBUG] df_np_raw npd_sku_count_sum={}', _debug_sum_numeric(df_np, 'npd_sku_count'))

        # 读取NP外部数据
        query_target = f"""
        select *
        from {target_db}.{np_target_table}	 
        where {mars_week_np_target_col} = '{mars_week}' 
        and {cal_channel_np_category_target_col} = 'TTL' {extra_np_target_condition}
        """
        logger.info(query_target)
        df_np_target = client.query_df(query_target)

        df_np_target = df_np_target.rename(columns=np_target_column_map)
        _debug_df_basic('df_np_target_after_rename', df_np_target, ['period', 'mars_week', 'npd_sku', 'cal_ncd_category', 'cal_ncd', 'sku_count_target', 'acuracy_rate', 'filter_out'])
        _debug_filter_out_distribution('df_np_target_after_rename', df_np_target)
        _debug_total_target('df_np_target_before_filter', df_np_target, 'npd_sku')
        np_filter_out_rules = build_filter_out_rules(df_np_target, sku_col='npd_sku')
        _debug_df_basic('np_filter_out_rules', np_filter_out_rules, ['npd_sku', 'cal_ncd_category', 'cal_ncd'])
        df_np = apply_filter_out_rules(df_np, np_filter_out_rules, sku_col='npd_sku')
        df_np_target = filter_target_rows(
            df_np_target,
            require_sku_target=False,
            sku_col='npd_sku',
            filter_out_rules=np_filter_out_rules,
        )
        _debug_df_basic('df_np_after_filter', df_np, ['period', 'mars_week', 'npd_sku', 'cal_ncd_category', 'cal_ncd', 'npd_sku_count'])
        _debug_df_basic('df_np_target_after_filter', df_np_target, ['period', 'mars_week', 'npd_sku', 'cal_ncd_category', 'cal_ncd', 'sku_count_target', 'acuracy_rate', 'filter_out'])
        _debug_total_target('df_np_target_after_filter', df_np_target, 'npd_sku')

        df_np_province = df_np.copy()
        df_np_region = df_np.groupby(['period', 'mars_week', 'npd_sku', 'cal_ncd_category']).agg(npd_sku_count=('npd_sku_count', 'sum')).reset_index()
        df_np_total = df_np.groupby(['period', 'mars_week', 'npd_sku']).agg(npd_sku_count=('npd_sku_count', 'sum')).reset_index()
        df_np_target_province = df_np_target[(~df_np_target['cal_ncd'].isna()) & (~df_np_target['cal_ncd_category'].isna())].reset_index(drop=True)
        df_np_target_region = df_np_target[(df_np_target['cal_ncd'].isna()) & (~df_np_target['cal_ncd_category'].isna())].reset_index(drop=True)
        df_np_target_total = df_np_target[(df_np_target['cal_ncd'].isna()) & (df_np_target['cal_ncd_category'].isna())].reset_index(drop=True)
        _debug_df_basic('df_np_target_province', df_np_target_province, ['period', 'mars_week', 'npd_sku', 'cal_ncd_category', 'cal_ncd', 'sku_count_target', 'acuracy_rate'])
        _debug_df_basic('df_np_target_region', df_np_target_region, ['period', 'mars_week', 'npd_sku', 'cal_ncd_category', 'cal_ncd', 'sku_count_target', 'acuracy_rate'])
        _debug_df_basic('df_np_target_total_detail', df_np_target_total, ['period', 'mars_week', 'npd_sku', 'sku_count_target', 'acuracy_rate', 'filter_out'])

        # 计算省份目标达成率
        target_join_colums = ['period','mars_week','npd_sku','cal_ncd_category','cal_ncd','sku_count_target','acuracy_rate']
        target_on_column = ['period', 'mars_week', 'npd_sku', 'cal_ncd_category','cal_ncd']
        df_np_province = df_np_province.merge(df_np_target_province[target_join_colums], on=target_on_column, how='left')
        columns_to_convert = ['npd_sku_count', 'sku_count_target', 'acuracy_rate']
        for col in columns_to_convert:
            df_np_province[col] = df_np_province[col].astype(float)
        adjusted_acuracy_rate = df_np_province['acuracy_rate'].fillna(1).copy()
        df_np_province['achievement_rate'] = df_np_province['npd_sku_count'] / df_np_province['sku_count_target']*adjusted_acuracy_rate
        df_np_province['achievement_rate'] = df_np_province['achievement_rate'].clip(upper=1.1)
        df_np_province['achievement_rate'] = df_np_province['achievement_rate'].round(4)

        # 计算区目标达成率
        target_join_colums.remove('cal_ncd')
        target_on_column.remove('cal_ncd')
        df_np_region = df_np_region.merge(df_np_target_region[target_join_colums], on=target_on_column, how='left')
        columns_to_convert = ['npd_sku_count', 'sku_count_target', 'acuracy_rate']
        for col in columns_to_convert:
            df_np_region[col] = df_np_region[col].astype(float)
        adjusted_acuracy_rate = df_np_region['acuracy_rate'].fillna(1).copy()
        df_np_region['achievement_rate'] = df_np_region['npd_sku_count'] / df_np_region['sku_count_target']*adjusted_acuracy_rate
        df_np_region['achievement_rate'] = df_np_region['achievement_rate'].clip(upper=1.1)
        df_np_region['achievement_rate'] = df_np_region['achievement_rate'].round(4)

        # 计算总目标达成率
        target_join_colums.remove('cal_ncd_category') 
        target_on_column.remove('cal_ncd_category')
        df_np_total = df_np_total.merge(df_np_target_total[target_join_colums], on=target_on_column, how='left')
        _debug_total_calc('df_np_total_after_merge_before_calc', df_np_total, 'npd_sku', 'npd_sku_count')
        columns_to_convert = ['npd_sku_count', 'sku_count_target', 'acuracy_rate']
        for col in columns_to_convert:
            df_np_total[col] = df_np_total[col].astype(float)
        adjusted_acuracy_rate = df_np_total['acuracy_rate'].fillna(1).copy()
        df_np_total['achievement_rate'] = df_np_total['npd_sku_count'] / df_np_total['sku_count_target']*adjusted_acuracy_rate
        df_np_total['achievement_rate'] = df_np_total['achievement_rate'].clip(upper=1.1)
        df_np_total['achievement_rate'] = df_np_total['achievement_rate'].round(4)
        _debug_total_calc('df_np_total_achievement', df_np_total, 'npd_sku', 'npd_sku_count')


        # 读取B5数据
        query = f"""
        select period, mars_week, b5_sku, cal_ncd_category, cal_ncd, sum(b5_sku_count) as b5_sku_count
        from store_b5_sku_ttl	 
        where mars_week = '{mars_week}' 
        and table_type = 'ttl' {extra_ttl_condition}
        group by period, mars_week, b5_sku, cal_ncd_category, cal_ncd
        """
        logger.info(query)
        df_b5 = client.query_df(query)
        _debug_df_basic('df_b5_raw', df_b5, ['period', 'mars_week', 'b5_sku', 'cal_ncd_category', 'cal_ncd', 'b5_sku_count'])
        logger.info('[DEBUG] df_b5_raw b5_sku_nunique={}', _debug_nunique(df_b5, 'b5_sku'))
        logger.info('[DEBUG] df_b5_raw b5_sku_count_sum={}', _debug_sum_numeric(df_b5, 'b5_sku_count'))

        # 读取B5外部数据
        query_target = f"""
        select *
        from {target_db}.{b5_target_table}	 
        where {mars_week_b5_target_col} = '{mars_week}' 
        and {cal_channel_b5_category_target_col} = 'TTL' {extra_b5_target_condition}
        """
        logger.info(query_target)
        df_b5_target = client.query_df(query_target)

        df_b5_target = df_b5_target.rename(columns=b5_target_column_map)
        _debug_df_basic('df_b5_target_after_rename', df_b5_target, ['period', 'mars_week', 'b5_sku', 'cal_ncd_category', 'cal_ncd', 'sku_count_target', 'acuracy_rate', 'filter_out'])
        _debug_filter_out_distribution('df_b5_target_after_rename', df_b5_target)
        _debug_total_target('df_b5_target_before_filter', df_b5_target, 'b5_sku')
        b5_filter_out_rules = build_filter_out_rules(df_b5_target, sku_col='b5_sku')
        _debug_df_basic('b5_filter_out_rules', b5_filter_out_rules, ['b5_sku', 'cal_ncd_category', 'cal_ncd'])
        df_b5 = apply_filter_out_rules(df_b5, b5_filter_out_rules, sku_col='b5_sku')
        df_b5_target = filter_target_rows(
            df_b5_target,
            require_sku_target=False,
            sku_col='b5_sku',
            filter_out_rules=b5_filter_out_rules,
        )
        _debug_df_basic('df_b5_after_filter', df_b5, ['period', 'mars_week', 'b5_sku', 'cal_ncd_category', 'cal_ncd', 'b5_sku_count'])
        _debug_df_basic('df_b5_target_after_filter', df_b5_target, ['period', 'mars_week', 'b5_sku', 'cal_ncd_category', 'cal_ncd', 'sku_count_target', 'acuracy_rate', 'filter_out'])
        _debug_total_target('df_b5_target_after_filter', df_b5_target, 'b5_sku')

        df_b5_province = df_b5.copy()
        df_b5_region = df_b5.groupby(['period', 'mars_week', 'b5_sku', 'cal_ncd_category']).agg(b5_sku_count=('b5_sku_count', 'sum')).reset_index()
        df_b5_total = df_b5.groupby(['period', 'mars_week', 'b5_sku']).agg(b5_sku_count=('b5_sku_count', 'sum')).reset_index()
        df_b5_target_province = df_b5_target[(~df_b5_target['cal_ncd'].isna()) & (~df_b5_target['cal_ncd_category'].isna())].reset_index(drop=True)
        df_b5_target_region = df_b5_target[(df_b5_target['cal_ncd'].isna()) & (~df_b5_target['cal_ncd_category'].isna())].reset_index(drop=True)
        df_b5_target_total = df_b5_target[(df_b5_target['cal_ncd'].isna()) & (df_b5_target['cal_ncd_category'].isna())].reset_index(drop=True)
        _debug_df_basic('df_b5_target_province', df_b5_target_province, ['period', 'mars_week', 'b5_sku', 'cal_ncd_category', 'cal_ncd', 'sku_count_target', 'acuracy_rate'])
        _debug_df_basic('df_b5_target_region', df_b5_target_region, ['period', 'mars_week', 'b5_sku', 'cal_ncd_category', 'cal_ncd', 'sku_count_target', 'acuracy_rate'])
        _debug_df_basic('df_b5_target_total_detail', df_b5_target_total, ['period', 'mars_week', 'b5_sku', 'sku_count_target', 'acuracy_rate', 'filter_out'])

        # 计算省份目标达成率
        target_join_colums = ['period','mars_week','b5_sku','cal_ncd_category','cal_ncd','sku_count_target','acuracy_rate']
        target_on_column = ['period', 'mars_week', 'b5_sku', 'cal_ncd_category','cal_ncd']
        df_b5_province = df_b5_province.merge(df_b5_target_province[target_join_colums], on=target_on_column, how='left')
        columns_to_convert = ['b5_sku_count', 'sku_count_target', 'acuracy_rate']
        for col in columns_to_convert:
            df_b5_province[col] = df_b5_province[col].astype(float)
        adjusted_acuracy_rate = df_b5_province['acuracy_rate'].fillna(1).copy()
        df_b5_province['achievement_rate'] = df_b5_province['b5_sku_count'] / df_b5_province['sku_count_target']*adjusted_acuracy_rate
        df_b5_province['achievement_rate'] = df_b5_province['achievement_rate'].clip(upper=1.1)
        df_b5_province['achievement_rate'] = df_b5_province['achievement_rate'].round(4)

        # 计算区目标达成率
        target_join_colums.remove('cal_ncd')
        target_on_column.remove('cal_ncd')
        df_b5_region = df_b5_region.merge(df_b5_target_region[target_join_colums], on=target_on_column, how='left')
        columns_to_convert = ['b5_sku_count', 'sku_count_target', 'acuracy_rate']
        for col in columns_to_convert:
            df_b5_region[col] = df_b5_region[col].astype(float)
        adjusted_acuracy_rate = df_b5_region['acuracy_rate'].fillna(1).copy()
        df_b5_region['achievement_rate'] = df_b5_region['b5_sku_count'] / df_b5_region['sku_count_target']*adjusted_acuracy_rate
        df_b5_region['achievement_rate'] = df_b5_region['achievement_rate'].clip(upper=1.1)
        df_b5_region['achievement_rate'] = df_b5_region['achievement_rate'].round(4)

        # 计算总目标达成率
        target_join_colums.remove('cal_ncd_category')
        target_on_column.remove('cal_ncd_category')
        df_b5_total = df_b5_total.merge(df_b5_target_total[target_join_colums], on=target_on_column, how='left')
        _debug_total_calc('df_b5_total_after_merge_before_calc', df_b5_total, 'b5_sku', 'b5_sku_count')
        columns_to_convert = ['b5_sku_count', 'sku_count_target', 'acuracy_rate']
        for col in columns_to_convert:
            df_b5_total[col] = df_b5_total[col].astype(float)
        adjusted_acuracy_rate = df_b5_total['acuracy_rate'].fillna(1).copy()
        df_b5_total['achievement_rate'] = df_b5_total['b5_sku_count'] / df_b5_total['sku_count_target']*adjusted_acuracy_rate
        df_b5_total['achievement_rate'] = df_b5_total['achievement_rate'].clip(upper=1.1)
        df_b5_total['achievement_rate'] = df_b5_total['achievement_rate'].round(4) 
        _debug_total_calc('df_b5_total_achievement', df_b5_total, 'b5_sku', 'b5_sku_count')

        # 计算平均值
        denominator = int(df_np['npd_sku'].nunique() + df_b5['b5_sku'].nunique())

        keep_cols = ['period','cal_ncd_category','cal_ncd','achievement_rate','sku_count_target']
        df_province = pd.concat([df_np_province[keep_cols],df_b5_province[keep_cols]], ignore_index=True)
        df_denominator = df_province.groupby(['period','cal_ncd_category','cal_ncd'])['sku_count_target'].count().reset_index()
        df_province = df_province.groupby(['period','cal_ncd_category','cal_ncd']).agg(achievement_rate=('achievement_rate', 'sum')).reset_index()
        df_province = pd.merge(df_province, df_denominator, on=['period','cal_ncd_category','cal_ncd'], how='left')
        df_province['achievement_rate'] = (df_province['achievement_rate'] / df_province['sku_count_target']).round(4)

        keep_cols = ['period','cal_ncd_category','achievement_rate','sku_count_target']
        df_region = pd.concat([df_np_region[keep_cols],df_b5_region[keep_cols]], ignore_index=True)
        df_denominator = df_region.groupby(['period','cal_ncd_category'])['sku_count_target'].count().reset_index()
        df_region = df_region.groupby(['period','cal_ncd_category']).agg(achievement_rate=('achievement_rate', 'sum')).reset_index()
        df_region = pd.merge(df_region, df_denominator, on=['period','cal_ncd_category'], how='left')
        df_region['achievement_rate'] = (df_region['achievement_rate'] / df_region['sku_count_target']).round(4)

        keep_cols = ['period','achievement_rate','sku_count_target']
        df_total = pd.concat([df_np_total[keep_cols],df_b5_total[keep_cols]], ignore_index=True)
        _debug_df_basic('final_df_total_before_agg', df_total, ['period', 'achievement_rate', 'sku_count_target'])
        logger.info('[DEBUG] final_df_total achievement_rate_sum_before_agg={}', _debug_sum_numeric(df_total, 'achievement_rate'))
        logger.info('[DEBUG] final_df_total sku_count_target_notna_count={}', _debug_count_notna(df_total, 'sku_count_target'))
        try:
            logger.info('[DEBUG] final_df_total sku_count_target_isna_count={}', int(df_total['sku_count_target'].isna().sum()))
        except Exception as e:
            logger.info('[DEBUG] final_df_total sku_count_target_isna_count_failed={}', repr(e))
        df_denominator = df_total.groupby(['period'])['sku_count_target'].count().reset_index()
        _debug_df_basic('final_df_denominator', df_denominator, ['period', 'sku_count_target'])
        df_total = df_total.groupby(['period']).agg(achievement_rate=('achievement_rate', 'sum')).reset_index()
        _debug_df_basic('final_df_total_sum_before_divide', df_total, ['period', 'achievement_rate'])
        df_total = pd.merge(df_total, df_denominator, on=['period'], how='left')
        df_total['achievement_rate'] = (df_total['achievement_rate'] / df_total['sku_count_target']).round(4)
        _debug_df_basic('final_df_total_after_divide', df_total, ['period', 'achievement_rate', 'sku_count_target'])

        df_result = pd.concat([df_province, df_region, df_total], ignore_index=True)

        condition1 = (df_result['cal_ncd_category'] == 'NNCD') & (~df_result['cal_ncd'].isna())
        condition2 = (df_result['cal_ncd_category'] == 'NCD') & (df_result['cal_ncd'].isna())
        condition3 = (df_result['cal_ncd_category'].isna())
        filtered_df = df_result[condition1 | condition2 | condition3].copy()

        filtered_df.loc[filtered_df['cal_ncd_category'] == 'NCD', 'cal_ncd'] = 'NCD'
        filtered_df = filtered_df.drop(columns=['cal_ncd_category','sku_count_target'])
        _debug_df_basic('filtered_df_final_output', filtered_df, ['period', 'cal_ncd', 'achievement_rate'])

        return genResultFromDf(filtered_df)
    except Exception as e:
        # 打印详细报错信息
        logger.error(traceback.print_exc())
    finally:
        # 关闭连接
        client.close()

def genResultFromDf(df_r: pd.DataFrame):
    # 构造输出
    label_map = {
        "period":"玛氏P",
        "cal_ncd":"NCD大区",
        "achievement_rate":"NCD大区必赢爆品B5/新品平均分销达成率"
                }
    data = []
    for v in list(df_r.values):
        r = []
        for cell in v:
            if type(cell) is pd._libs.missing.NAType or pd.isna(cell):
                cell = None
            r.append(cell)
        data.append(r)
    r = {'fields': [{'name': c, 'type': 'string','label':label_map[c]} for c in df_r.columns],
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
