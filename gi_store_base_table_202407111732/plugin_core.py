import datetime
import os
import pandas as pd
from table_wrapper import wrapper
from catalog import DataCatalog
from loguru import logger
import clickhouse_connect
from clickhouse_connect.driver.tools import insert_file
from hbase.operate import HbaseClient
from plugin_config import ENV, app_key, app_secret, fs_root_dir, io_column_dict, clickhouse_connect_params
import shutil
import json
log_format = '[{time:YYYY-MM-DD} {time:HH:mm:ss}][{file}:{line}][{level}] -> {message}'
log_sink = "%s_{time}.log" % ('full_course')
logger.add(log_sink, level='INFO', format=log_format)


def collect_df_from_csv(table_name: str, files: list, column_mapping: dict):
    dfs = []
    for i in files:
        dfs.append(pd.read_csv(i, sep="\t", dtype=str))
    return pd.concat(dfs, axis=0)[column_mapping[table_name]]

def get_ck_connections(clickhouse_connect_params: dict) -> dict:
    target_db = clickhouse_connect_params['CLICKHOUSE_DB'].split(';')
    conn = {}
    for i in target_db:
        client = clickhouse_connect.get_client(host=clickhouse_connect_params['CLICKHOUSE_HOST'],
                                                   user=clickhouse_connect_params['CLICKHOUSE_USER'],
                                                   password=clickhouse_connect_params['CLICKHOUSE_PASSWORD'],
                                                   port=clickhouse_connect_params['CLICKHOUSE_PORT'],
                                                   database=i)
        conn[i] = client
    return conn


def read_csv_create_id_then_insert_file(clickhouse_cli, csv_path, table, database, csv_path_as_df=False):
    setting = {'input_format_allow_errors_ratio': 0,
               'input_format_allow_errors_num': 0}
    if csv_path_as_df:
        df = csv_path
    else:
        df = pd.read_csv(csv_path)
    # df.astype(str)
    # df.to_csv('tmp_insert1.csv', index=False)
    # df = pd.read_csv('tmp_insert1.csv', dtype=str)
    df.to_csv('tmp_insert.csv', index=False)
    insert_file(clickhouse_cli, table, 'tmp_insert.csv', database=database, settings=setting)


def main(params):
    body = json.loads(params['body']['params'])
    from_local = body.get("from_local", False)
    # otc_list = body.get("otc_list", ['EBB OTC', 'EWM OTC', 'DMT OTC', 'DMU OTC', 'DMRS OTC', 'ESB OTC'])
    # douyin_customer_list = body.get("douyin_customer_list", ['17596400','17611564','17614953','17617665','17619197'])
    calc_date = body.get('calc_date',None)
    # from_local = True
    # otc_list = ['EBB OTC', 'EWM OTC', 'DMT OTC', 'DMU OTC', 'DMRS OTC', 'ESB OTC']
    # douyin_customer_list = ['17596400','17611564','17614953','17617665','17619197']
    hb_client = HbaseClient(app_key, app_secret, fs_root_dir)
    catalog = DataCatalog(hb_client, logger)
    folder_path = 'export_files'

    # 使用os.path.exists()函数判断文件夹是否存在
    if not from_local and os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        
    # YESTERDAY = (datetime.datetime(2024,5,19) - datetime.timedelta(days=1)).strftime('%Y%m%d')
    if calc_date is None:
        YESTERDAY = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    else:
        YESTERDAY = calc_date
    calendar_files = catalog.load_data_from_hbase('l0_mdp.mars_calendar', from_local=from_local)
    calendar_df = collect_df_from_csv('l0_mdp.mars_calendar', calendar_files, column_mapping=io_column_dict)

    row = calendar_df.loc[calendar_df['dataid'] == YESTERDAY].iloc[0]
    PERIOD = row['m_year'] + row['m_period'].zfill(2)
    PERIOD_OUTPUT = row['m_year'] + 'P' + row['m_period'].zfill(2)

    logger.info(f"Running params: YESTERDAY: {YESTERDAY}, PERIOD: {PERIOD}, ENV: {ENV}")

    logger.info("开始加载CMT数据")

    # cmt_geo_files = catalog.load_data_from_hbase('l0_cmt.geography', from_local=from_local)
    # cmt_geo_df = collect_df_from_csv('l0_cmt.geography', cmt_geo_files, column_mapping=io_column_dict)
    # cmt_geo_files_fz = catalog.load_data_from_hbase('l0_cmt.geography_freeze', period=PERIOD, from_local=from_local)
    # cmt_geo_df_fz = collect_df_from_csv('l0_cmt.geography_freeze', cmt_geo_files_fz, column_mapping=io_column_dict)

    # cmt_customer_files = catalog.load_data_from_hbase('l0_cmt.customer', from_local=from_local)
    # cmt_customer_df = collect_df_from_csv('l0_cmt.customer', cmt_customer_files, column_mapping=io_column_dict)

    # cmt_customer_files_fz = catalog.load_data_from_hbase('l0_cmt.customer_freeze', period=PERIOD, from_local=from_local)
    # cmt_customer_df_fz = collect_df_from_csv('l0_cmt.customer_freeze', cmt_customer_files_fz,
    #                                          column_mapping=io_column_dict)

    # cmt_calculate_sellout_files = catalog.load_data_from_hbase('l0_cmt.calculate_sellout', period=PERIOD,
    #                                                            from_local=from_local)
    # cmt_calculate_sellout_df = collect_df_from_csv('l0_cmt.calculate_sellout', cmt_calculate_sellout_files,
    #                                                column_mapping=io_column_dict)
    

    # cmt_product_files = catalog.load_data_from_hbase('l0_cmt.product', from_local=from_local)
    # cmt_product_df = collect_df_from_csv('l0_cmt.product', cmt_product_files, column_mapping=io_column_dict)

    # cmt_inventory_files = catalog.load_data_from_hbase('l0_cmt.inventory', period=PERIOD, from_local=from_local)
    # cmt_inventory_df = collect_df_from_csv('l0_cmt.inventory', cmt_inventory_files, column_mapping=io_column_dict)

    # cmt_sellin_files = catalog.load_data_from_hbase('l0_cmt.sellin', period=PERIOD, from_local=from_local)
    # cmt_sellin_df = collect_df_from_csv('l0_cmt.sellin', cmt_sellin_files, column_mapping=io_column_dict)

    # cmt_segment_files = catalog.load_data_from_hbase('l0_cmt.segment', from_local=from_local)
    # cmt_segment_df = collect_df_from_csv('l0_cmt.segment', cmt_segment_files, column_mapping=io_column_dict)

    # cmt_subsegment_files = catalog.load_data_from_hbase('l0_cmt.subSegment', from_local=from_local)
    # cmt_subsegment_df = collect_df_from_csv('l0_cmt.subSegment', cmt_subsegment_files, column_mapping=io_column_dict)
    # cmt_calculate_sellout_df['Period'] = '202406'

    logger.info("CMT数据加载完毕")

    logger.info("开始加载完美门店截P数据")
    all_p_files = catalog.load_data_from_hbase('l2_cot_perfect_store.zo_all_p_2024', period=PERIOD_OUTPUT,
                                               from_local=from_local)
    all_p_df = collect_df_from_csv('l2_cot_perfect_store.zo_all_p_2024', all_p_files,
                                   column_mapping=io_column_dict)

    bysku_files = catalog.load_data_from_hbase('l2_cot_perfect_store.zo_bysku_detail_2024_p', period=PERIOD_OUTPUT,
                                               from_local=from_local)
    bysku_df = collect_df_from_csv('l2_cot_perfect_store.zo_bysku_detail_2024_p', bysku_files,
                                   column_mapping=io_column_dict)

    cvs_files = catalog.load_data_from_hbase('l2_cot_perfect_store.zo_cvs_detail_2024_p', period=PERIOD_OUTPUT,
                                             from_local=from_local)
    cvs_df = collect_df_from_csv('l2_cot_perfect_store.zo_cvs_detail_2024_p', cvs_files, column_mapping=io_column_dict)

    mini_files = catalog.load_data_from_hbase('l2_cot_perfect_store.zo_minijy_detail_2024_p', period=PERIOD_OUTPUT,
                                              from_local=from_local)
    mini_df = collect_df_from_csv('l2_cot_perfect_store.zo_minijy_detail_2024_p', mini_files,
                                  column_mapping=io_column_dict)

    tt_files = catalog.load_data_from_hbase('l2_cot_perfect_store.zo_tt_detail_2024_p', period=PERIOD_OUTPUT,
                                            from_local=from_local)
    tt_df = collect_df_from_csv('l2_cot_perfect_store.zo_tt_detail_2024_p', tt_files, column_mapping=io_column_dict)

    ws_files = catalog.load_data_from_hbase('l2_cot_perfect_store.zo_ws_detail_2024_p', period=PERIOD_OUTPUT,
                                            from_local=from_local)
    ws_df = collect_df_from_csv('l2_cot_perfect_store.zo_ws_detail_2024_p', ws_files, column_mapping=io_column_dict)

    ps_all_files = catalog.load_data_from_hbase('l2_cot_perfect_store.zo_ps_all_detail_2024_p', period=PERIOD_OUTPUT,
                                                from_local=from_local)
    ps_all_df = collect_df_from_csv('l2_cot_perfect_store.zo_ps_all_detail_2024_p', ps_all_files,
                                    column_mapping=io_column_dict)
    
    # 收集项门店编码字段为hsp_field010，统一转成code
    proj_east = catalog.load_data_from_hbase('l2_cot_perfect_store.zo_proj_form_east_2024_p', period=PERIOD_OUTPUT,
                                             from_local=from_local)
    proj_east_df = collect_df_from_csv('l2_cot_perfect_store.zo_proj_form_east_2024_p', proj_east,
                                       column_mapping=io_column_dict).rename(columns={'hsp_field010':'code'})

    proj_north = catalog.load_data_from_hbase('l2_cot_perfect_store.zo_proj_form_north_2024_p', period=PERIOD_OUTPUT,
                                              from_local=from_local)
    proj_north_df = collect_df_from_csv('l2_cot_perfect_store.zo_proj_form_north_2024_p', proj_north,
                                        column_mapping=io_column_dict).rename(columns={'hsp_field010':'code'})

    proj_tt = catalog.load_data_from_hbase('l2_cot_perfect_store.zo_proj_form_tt_2024_p', period=PERIOD_OUTPUT,
                                           from_local=from_local)
    proj_tt_df = collect_df_from_csv('l2_cot_perfect_store.zo_proj_form_tt_2024_p', proj_tt,
                                     column_mapping=io_column_dict).rename(columns={'hsp_field010':'code'})

    logger.info("完美门店截P数据加载完毕")
    logger.info("开始生成门店维度报表：execute_king_nationwide")
    r1 = wrapper(logger, report_name='execute_king_nationwide',
                 report_params=(all_p_df, bysku_df, cvs_df, mini_df, tt_df, ws_df, ps_all_df,
                                proj_east_df, proj_north_df, proj_tt_df))
    if r1[0] == 'OK':
        logger.info(f"Done 生成门店维度报表：execute_king_nationwide，shape is {r1[2].shape}")
        execute_king_nationwide = r1[2]
        execute_king_nationwide.to_csv('execute_king_nationwide.csv', index=False)
    else:
        logger.error("处理报错，详细情况如下：")
        logger.error(r1[2])
    logger.info("\n")

    # logger.info("开始生成客户维度报表：soldto_gum_indicator")
    # r1 = wrapper(logger, report_name='soldto_gum_indicator',
    #              report_params=(PERIOD, otc_list, calendar_df, cmt_geo_df, cmt_calculate_sellout_df, cmt_segment_df,
    #                             cmt_customer_df, cmt_inventory_df, cmt_sellin_df, cmt_subsegment_df, cmt_product_df))
    # if r1[0] == 'OK':
    #     logger.info(f"Done 生成门店维度报表：soldto_gum_indicator，shape is {r1[2].shape}")
    #     soldto_gum_indicator = r1[2]
    #     soldto_gum_indicator['period'] = PERIOD_OUTPUT
    #     soldto_gum_indicator.to_csv('soldto_gum_indicator.csv', index=False)
    # else:
    #     logger.error("处理报错，详细情况如下：")
    #     logger.error(r1[2])
    # logger.info("\n")
    #
    # logger.info("开始生成战队维度报表：team_soldto_gum_indicator")
    # r1 = wrapper(logger, report_name='team_soldto_gum_indicator',
    #              report_params=(PERIOD, otc_list, douyin_customer_list, calendar_df, cmt_geo_df_fz, cmt_calculate_sellout_df, cmt_segment_df,
    #                             cmt_customer_df_fz, cmt_inventory_df, cmt_sellin_df, cmt_subsegment_df, cmt_product_df))
    # if r1[0] == 'OK':
    #     logger.info(f"Done 生成战队维度报表：team_soldto_gum_indicator，shape is {r1[2].shape}")
    #     team_soldto_gum_indicator = r1[2]
    #     team_soldto_gum_indicator['period'] = PERIOD_OUTPUT
    #     team_soldto_gum_indicator.to_csv('team_soldto_gum_indicator.csv', index=False)
    # else:
    #     logger.error("处理报错，详细情况如下：")
    #     logger.error(r1[2])
    # logger.info("\n")
    #
    # logger.info("开始生成地理维度报表：geography_gum_indicator")
    # execute_king_nationwide_copy = execute_king_nationwide.copy()
    # r1 = wrapper(logger, report_name='geography_gum_indicator',
    #              report_params=(soldto_gum_indicator,execute_king_nationwide_copy,cmt_geo_df))
    # if r1[0] == 'OK':
    #     logger.info(f"Done 生成地理维度报表：geography_gum_indicator，shape is {r1[2].shape}")
    #     geography_gum_indicator = r1[2]
    #     geography_gum_indicator['period'] = PERIOD_OUTPUT
    #     geography_gum_indicator.to_csv('geography_gum_indicator.csv', index=False)
    # else:
    #     logger.error("处理报错，详细情况如下：")
    #     logger.error(r1[2])
    # logger.info("\n")

    logger.info("所有数据生成完毕，开始入库")
    clients: dict = get_ck_connections(clickhouse_connect_params)
    logger.info("Doing execute_king_nationwide")
    clients['supervisor_dashboard'].command(f"ALTER TABLE execute_king_nationwide DELETE WHERE period = '{PERIOD_OUTPUT}'")
    read_csv_create_id_then_insert_file(clients['supervisor_dashboard'], execute_king_nationwide,
                                        'execute_king_nationwide','supervisor_dashboard', True)
    logger.info("Done\n")

    # logger.info("Doing soldto_gum_indicator")
    # clients['sv_gum_indicator'].command(f"ALTER TABLE soldto_gum_indicator DELETE WHERE period = '{PERIOD_OUTPUT}'")
    # read_csv_create_id_then_insert_file(clients['sv_gum_indicator'], soldto_gum_indicator,
    #                                     'soldto_gum_indicator','sv_gum_indicator', True)
    # logger.info("Done\n")
    #
    # logger.info("Doing team_soldto_gum_indicator")
    # clients['sv_gum_indicator'].command(f"ALTER TABLE team_soldto_gum_indicator DELETE WHERE period = '{PERIOD_OUTPUT}'")
    # read_csv_create_id_then_insert_file(clients['sv_gum_indicator'], team_soldto_gum_indicator,
    #                                     'team_soldto_gum_indicator','sv_gum_indicator', True)
    # logger.info("Done\n")
    #
    # logger.info("Doing geography_gum_indicator")
    # clients['sv_gum_indicator'].command(f"ALTER TABLE geography_gum_indicator DELETE WHERE period = '{PERIOD_OUTPUT}'")
    # read_csv_create_id_then_insert_file(clients['sv_gum_indicator'], geography_gum_indicator,
    #                                     'geography_gum_indicator','sv_gum_indicator', True)
    # logger.info("Done\n")
    logger.info("入库完毕")
    
    for k in clients:
        clients[k].close()


