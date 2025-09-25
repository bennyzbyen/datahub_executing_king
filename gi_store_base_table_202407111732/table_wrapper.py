import traceback

from tables.store_table import StoreDim
from tables.geo_table import GeoDim
from tables.customer_table import CustomerDim
from tables.team_table import TeamDim


def wrapper(logger: any, report_name: str, report_params: any) -> tuple:
    logger.info(f"Data generation process for {report_name} is ready")
    logger.info("===================================================")
    if report_name == "execute_king_nationwide":
        store_dim = StoreDim(logger)
        try:
            (all_p_df, bysku_df, cvs_df, mini_df, tt_df, ws_df, ps_all_df,
             proj_east_df, proj_north_df, proj_tt_df) = report_params
            final = store_dim.handle(all_p_df, bysku_df, cvs_df, mini_df, tt_df,
                                     ws_df, ps_all_df, proj_east_df, proj_north_df, proj_tt_df)
            logger.info("===================================================\n")
            return ('OK', report_name, final)
        except Exception:
            err = traceback.format_exc()
            logger.error(err)
            logger.info("===================================================\n")
            return ('ERROR', report_name, err)
    elif report_name == 'geography_gum_indicator':
        geo_dim = GeoDim(logger)
        try:
            (customer_df, execute_king_nationwide_df,geography_df) = report_params
            final = geo_dim.handle(customer_df, execute_king_nationwide_df, geography_df)
            logger.info("===================================================\n")
            return ('OK', report_name, final)
        except Exception:
            err = traceback.format_exc()
            logger.error(err)
            logger.info("===================================================\n")
            return ('ERROR', report_name, err)
    elif report_name == 'soldto_gum_indicator':
        (period, otc_list, calendar_df, cmt_geo_df, cmt_calculate_sellout_df, cmt_segment_df,
         cmt_customer_df, cmt_inventory_df, cmt_sellin_df, cmt_subsegment_df, cmt_product_df) = report_params
        customer_dim = CustomerDim(period, otc_list, calendar_df, cmt_geo_df, cmt_calculate_sellout_df, cmt_segment_df,
                                   cmt_customer_df, cmt_inventory_df, cmt_sellin_df, cmt_subsegment_df, cmt_product_df,
                                   logger)
        try:
            final = customer_dim.handle()
            logger.info("===================================================\n")
            return ('OK', report_name, final)
        except Exception:
            err = traceback.format_exc()
            logger.error(err)
            logger.info("===================================================\n")
            return ('ERROR', report_name, err)
    elif report_name == 'team_soldto_gum_indicator':
        (period, otc_list, douyin_customer_list, calendar_df, cmt_geo_df, cmt_calculate_sellout_df, cmt_segment_df,
         cmt_customer_df, cmt_inventory_df, cmt_sellin_df, cmt_subsegment_df, cmt_product_df) = report_params
        team_dim = TeamDim(period, otc_list, douyin_customer_list, calendar_df, cmt_geo_df, cmt_calculate_sellout_df, cmt_segment_df,
                           cmt_customer_df, cmt_inventory_df, cmt_sellin_df, cmt_subsegment_df, cmt_product_df,
                           logger)
        try:
            final = team_dim.handle()
            logger.info("===================================================\n")
            return ('OK', report_name, final)
        except Exception:
            err = traceback.format_exc()
            logger.error(err)
            logger.info("===================================================\n")
            return ('ERROR', report_name, err)
    else:
        pass
