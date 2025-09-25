# 客户维度底表
import datetime
import pandas as pd


class CustomerDim(object):
    def __init__(self, period: str, otc_list:list, calendar_df: pd.DataFrame, cmt_geo_df: pd.DataFrame, cmt_calculate_sellout_df: pd.DataFrame,cmt_segment_df: pd.DataFrame,cmt_customer_df: pd.DataFrame,cmt_inventory_df: pd.DataFrame,cmt_sellin_df: pd.DataFrame,cmt_subsegment_df: pd.DataFrame,cmt_product_df: pd.DataFrame,logger: any):
        self.logger = logger
        self.Period = period
        self.OTC_list = otc_list
        self.calendar_df = calendar_df
        self.cmt_geo_df = cmt_geo_df
        self.cmt_calculate_sellout_df = cmt_calculate_sellout_df
        self.cmt_segment_df = cmt_segment_df
        self.cmt_customer_df = cmt_customer_df
        self.cmt_inventory_df = cmt_inventory_df
        self.cmt_sellin_df = cmt_sellin_df
        self.cmt_subsegment_df = cmt_subsegment_df
        self.cmt_product_df = cmt_product_df
        self.get_inventory_date_id()
        self.output_columns = [
            'update_time', 'last_update_time',  'period',
            'mars_region_code', 'mars_region_name', 'mars_province_code', 'mars_province_name',
            'mars_city_cluster_code', 'mars_city_cluster_name', 'mars_city_code', 'mars_city_name',
            'customer_no', 'customer_name', 'customer_group_code', 'customer_group_name',
            'customer_segment', 'customer_type', 'customer_attribute', 'gi_ptd_sellout_gsv',
            'gi_gt_inv_gsv', 'gi_ptd_gum_gsv_value', 'gi_ptd_gum_dts_value', 'gi_ptd_otc_gsv_value',
            'gi_ptd_otc_gsv_vol', 'gi_ptd_otc_sellout_value', 'gi_ptd_otc_sellout_vol'
        ]

    def get_inventory_date_id(self):
        self.inventory_date_id = str(self.cmt_inventory_df['DateID'].max())
        pass

    def handle(self):
        df = self.cmt_customer_df.assign(period=self.Period)
        df = df[['period','CustomerID','CustomerNo', 'CustomerName','CustomerGroupCode', 'CustomerGroupName', 'CustomerSegment','CustomerType', 'CustomerAttribute']].rename(columns={'CustomerNo':'customer_no','CustomerName':'customer_name','CustomerGroupCode':'customer_group_code','CustomerGroupName':'customer_group_name','CustomerSegment':'customer_segment','CustomerType':'customer_type','CustomerAttribute':'customer_attribute',})
        df1 = self.gi_ptd_sellout_gsv()
        df2 = self.gi_gt_inv_gsv()
        df3 = self.gi_ptd_gum_gsv_value()
        df4 = self.gi_ptd_gum_dts_value()
        df5 = self.gi_ptd_otc_gsv_value()
        df6 = self.gi_ptd_otc_gsv_vol()
        df7 = self.gi_ptd_otc_sellout_value()
        df8 = self.gi_ptd_otc_sellout_vol()
        df_duplicates = pd.concat([df1[['period','CustomerID','GeographyID']],df2[['period','CustomerID','GeographyID']],df3[['period','CustomerID','GeographyID']],df4[['period','CustomerID','GeographyID']],df5[['period','CustomerID','GeographyID']],df6[['period','CustomerID','GeographyID']],df7[['period','CustomerID','GeographyID']],df8[['period','CustomerID','GeographyID']]]).drop_duplicates()
        df = pd.merge(df,df_duplicates,how='left',on=['period','CustomerID'])
        df = pd.merge(df,df1,how='left',on=['period','CustomerID','GeographyID'])
        df = pd.merge(df,df2,how='left',on=['period','CustomerID','GeographyID'])
        df = pd.merge(df,df3,how='left',on=['period','CustomerID','GeographyID'])
        df = pd.merge(df,df4,how='left',on=['period','CustomerID','GeographyID'])
        df = pd.merge(df,df5,how='left',on=['period','CustomerID','GeographyID'])
        df = pd.merge(df,df6,how='left',on=['period','CustomerID','GeographyID'])
        df = pd.merge(df,df7,how='left',on=['period','CustomerID','GeographyID'])
        df = pd.merge(df,df8,how='left',on=['period','CustomerID','GeographyID'])
        df = df[(~df['gi_ptd_sellout_gsv'].isna())|(~df['gi_gt_inv_gsv'].isna())|(~df['gi_ptd_gum_gsv_value'].isna())|(~df['gi_ptd_gum_dts_value'].isna())|(~df['gi_ptd_otc_gsv_value'].isna())|(~df['gi_ptd_otc_gsv_vol'].isna())|(~df['gi_ptd_otc_sellout_value'].isna())|(~df['gi_ptd_otc_sellout_vol'].isna())]
        df = pd.merge(df,self.cmt_geo_df[['GeographyID', 'RegionCode', 'RegionName', 'ProvinceCode_G', 'ProvinceName_G',
                                'CitygroupCode', 'CitygroupName', 'CityCode_G', 'CityName_G']],how='outer',on=['GeographyID'])

        df = df.rename(columns={
            'period': 'period',
             'RegionCode': 'mars_region_code',
             'RegionName': 'mars_region_name',
             'ProvinceCode_G': 'mars_province_code',
             'ProvinceName_G': 'mars_province_name',
             'CitygroupCode': 'mars_city_cluster_code',
             'CitygroupName': 'mars_city_cluster_name',
             'CityCode_G': 'mars_city_code',
             'CityName_G': 'mars_city_name',
             'CustomerNo': 'customer_no',
             'CustomerName': 'customer_name',
             'CustomerGroupCode': 'customer_group_code',
             'CustomerGroupName': 'customer_group_name',
             'CustomerSegment': 'customer_segment',
             'CustomerType': 'customer_type',
             'CustomerAttribute': 'customer_attribute'
        })
        for c in ['gi_ptd_sellout_gsv','gi_gt_inv_gsv','gi_ptd_gum_gsv_value','gi_ptd_gum_dts_value','gi_ptd_otc_gsv_value','gi_ptd_otc_gsv_vol','gi_ptd_otc_sellout_value','gi_ptd_otc_sellout_vol']:
            df.loc[:,c] = df[c].fillna(0)
        for c in ['customer_group_code','customer_group_name','customer_segment','customer_type','customer_attribute','customer_no','customer_name','mars_region_code','mars_region_name','mars_province_code','mars_province_name','mars_city_cluster_code','mars_city_cluster_name','mars_city_code','mars_city_name',]:
            df.loc[:,c] = df[c].fillna('-')
        df['update_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['last_update_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['cl_last_update_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        return df[self.output_columns]


    def gi_ptd_sellout_gsv(self):
        df = self.cmt_calculate_sellout_df[self.cmt_calculate_sellout_df['Period']==self.Period]
        df = pd.merge(df,self.cmt_segment_df[self.cmt_segment_df['SegmentName'].isin(['巧克力','箭牌'])],how='inner',on=['SegmentID'])
        df = pd.merge(df,self.cmt_customer_df[['CustomerID', 'CustomerNo', 'CustomerName']],how='left',on=['CustomerID'])
        df = pd.merge(df,self.cmt_geo_df[['GeographyID', 'RegionName', 'ProvinceName_G', 'CitygroupName','CityName_G']],how='left',on=['GeographyID'])
        if df.empty:
            df = pd.DataFrame(columns = ['period', 'CustomerID','GeographyID', 'gi_ptd_sellout_gsv'])
        else:
            df.loc[:,"CALCULATE_GSV"] = df['CALCULATE_GSV'].astype(float)
            df = df.groupby(['Period','RegionName','ProvinceName_G','CitygroupName','CityName_G','CustomerNo','CustomerName','CustomerID','GeographyID'],dropna=False)['CALCULATE_GSV'].sum().reset_index()
            df.columns = ['period', 'mars_region_name', 'mars_province_name', 'mars_city_cluster_name', 'mars_city_name',
                'customer_no', 'customer_name','CustomerID','GeographyID', 'gi_ptd_sellout_gsv']
            df = df[['period', 'CustomerID','GeographyID', 'gi_ptd_sellout_gsv']]
        return df
    def gi_gt_inv_gsv(self):
        df = self.cmt_inventory_df[self.cmt_inventory_df['DateID']==self.inventory_date_id]
        df = pd.merge(df,self.cmt_segment_df[self.cmt_segment_df['SegmentName'].isin(['巧克力','箭牌'])],how='inner',on=['SegmentID'])
        df = pd.merge(df,self.cmt_customer_df[['CustomerID', 'CustomerNo', 'CustomerName']],how='left',on=['CustomerID'])
        df = pd.merge(df,self.cmt_geo_df[['GeographyID', 'RegionName', 'ProvinceName_G', 'CitygroupName','CityName_G']],how='left',on=['GeographyID'])
        if df.empty:
            df = pd.DataFrame(columns = ['period','CustomerID','GeographyID', 'gi_gt_inv_gsv'])
        else:
            df.loc[:,"FINALINV_GSV"] = df['FINALINV_GSV'].astype(float)
            df = df.groupby(['P_Freeze','CustomerNo','CustomerName','RegionName','ProvinceName_G','CitygroupName','CityName_G','CustomerID','GeographyID'],dropna=False)['FINALINV_GSV'].sum().reset_index()
            df.columns = ['period', 'customer_no', 'customer_name', 'mars_region_name', 'mars_province_name', 'mars_city_cluster_name', 'mars_city_name','CustomerID','GeographyID', 'gi_gt_inv_gsv']
            df = df[['period','CustomerID','GeographyID', 'gi_gt_inv_gsv']]
        return df
    def gi_ptd_gum_gsv_value(self):
        df = self.cmt_sellin_df[self.cmt_sellin_df['P_Freeze']==self.Period]
        df = pd.merge(df,self.cmt_segment_df[self.cmt_segment_df['SegmentName'].isin(['巧克力','箭牌'])][['SegmentID']],how='inner',on=['SegmentID'])
        df = pd.merge(df,self.cmt_customer_df[['CustomerID', 'CustomerNo', 'CustomerName']],how='left',on=['CustomerID'])
        df = pd.merge(df,self.cmt_geo_df[['GeographyID', 'RegionName', 'ProvinceName_G', 'CitygroupName','CityName_G']],how='left',on=['GeographyID'])
        df = pd.merge(df,self.cmt_subsegment_df[self.cmt_subsegment_df['SubSegmentName'].isin(['GUM'])][['SubSegmentID']],how='inner',on=['SubSegmentID'])
        if df.empty:
            df = pd.DataFrame(columns = ['period','CustomerID','GeographyID', 'gi_ptd_gum_gsv_value'])
        else:
            df.loc[:,"ShipmentAmount"] = df['ShipmentAmount'].astype(float)
            df = df.groupby(['P_Freeze','CustomerNo','CustomerName','RegionName','ProvinceName_G','CitygroupName','CityName_G','CustomerID','GeographyID'],dropna=False)['ShipmentAmount'].sum().reset_index()
            df.columns = ['period', 'customer_no', 'customer_name', 'mars_region_name', 'mars_province_name', 'mars_city_cluster_name', 'mars_city_name','CustomerID','GeographyID', 'gi_ptd_gum_gsv_value']
            df = df[['period','CustomerID','GeographyID', 'gi_ptd_gum_gsv_value']]
        return df
    def gi_ptd_gum_dts_value(self):
        df = self.cmt_calculate_sellout_df[self.cmt_calculate_sellout_df['Period']==self.Period]
        df = pd.merge(df,self.cmt_segment_df[self.cmt_segment_df['SegmentName'].isin(['巧克力','箭牌'])],how='inner',on=['SegmentID'])
        df = pd.merge(df,self.cmt_customer_df[['CustomerID', 'CustomerNo', 'CustomerName']],how='left',on=['CustomerID'])
        df = pd.merge(df,self.cmt_geo_df[['GeographyID', 'RegionName', 'ProvinceName_G', 'CitygroupName','CityName_G']],how='left',on=['GeographyID'])
        df = pd.merge(df,self.cmt_subsegment_df[self.cmt_subsegment_df['SubSegmentName'].isin(['GUM'])][['SubSegmentID']],how='inner',on=['SubSegmentID'])
        if df.empty:
            df = pd.DataFrame(columns = ['period','CustomerID','GeographyID', 'gi_ptd_gum_dts_value'])
        else:
            df.loc[:,"CALCULATE_GSV"] = df['CALCULATE_GSV'].astype(float)
            df = df.groupby(['Period','RegionName','ProvinceName_G','CitygroupName','CityName_G','CustomerNo','CustomerName','CustomerID','GeographyID'],dropna=False)['CALCULATE_GSV'].sum().reset_index()
            df.columns = ['period', 'mars_region_name', 'mars_province_name', 'mars_city_cluster_name', 'mars_city_name',
                'customer_no', 'customer_name','CustomerID','GeographyID', 'gi_ptd_gum_dts_value']
            df = df[['period','CustomerID','GeographyID', 'gi_ptd_gum_dts_value']]
        return df
    def gi_ptd_otc_gsv_value(self):
        df = self.cmt_sellin_df[self.cmt_sellin_df['P_Freeze']==self.Period]
        df = pd.merge(df,self.cmt_segment_df[self.cmt_segment_df['SegmentName'].isin(['巧克力','箭牌'])],how='inner',on=['SegmentID'])
        df = pd.merge(df,self.cmt_customer_df[['CustomerID', 'CustomerNo', 'CustomerName']],how='left',on=['CustomerID'])
        df = pd.merge(df,self.cmt_geo_df[['GeographyID', 'RegionName', 'ProvinceName_G', 'CitygroupName','CityName_G']],how='left',on=['GeographyID'])
        df = pd.merge(df,self.cmt_subsegment_df[self.cmt_subsegment_df['SubSegmentName'].isin(['GUM'])][['SubSegmentID']],how='inner',on=['SubSegmentID'])
        df = pd.merge(df,self.cmt_product_df[self.cmt_product_df['CHINA_FORECAST_GROUP_DESC'].isin(self.OTC_list)][['ProductID']],how='inner',on=['ProductID'])

        if df.empty:
            df = pd.DataFrame(columns = ['period','CustomerID','GeographyID', 'gi_ptd_otc_gsv_value'])
        else:
            df.loc[:,"ShipmentAmount"] = df['ShipmentAmount'].astype(float)
            df = df.groupby(['P_Freeze','CustomerNo','CustomerName','RegionName','ProvinceName_G','CitygroupName','CityName_G','CustomerID','GeographyID'],dropna=False)['ShipmentAmount'].sum().reset_index()
            df.columns = ['period', 'customer_no', 'customer_name', 'mars_region_name', 'mars_province_name', 'mars_city_cluster_name', 'mars_city_name','CustomerID','GeographyID', 'gi_ptd_otc_gsv_value']
            df = df[['period','CustomerID','GeographyID', 'gi_ptd_otc_gsv_value']]
        return df
    def gi_ptd_otc_gsv_vol(self):
        df = self.cmt_sellin_df[self.cmt_sellin_df['P_Freeze']==self.Period]
        df = pd.merge(df,self.cmt_segment_df[self.cmt_segment_df['SegmentName'].isin(['巧克力','箭牌'])],how='inner',on=['SegmentID'])
        df = pd.merge(df,self.cmt_customer_df[['CustomerID', 'CustomerNo', 'CustomerName']],how='left',on=['CustomerID'])
        df = pd.merge(df,self.cmt_geo_df[['GeographyID', 'RegionName', 'ProvinceName_G', 'CitygroupName','CityName_G']],how='left',on=['GeographyID'])
        df = pd.merge(df,self.cmt_subsegment_df[self.cmt_subsegment_df['SubSegmentName'].isin(['GUM'])][['SubSegmentID']],how='inner',on=['SubSegmentID'])
        df = pd.merge(df,self.cmt_product_df[self.cmt_product_df['CHINA_FORECAST_GROUP_DESC'].isin(self.OTC_list)][['ProductID']],how='inner',on=['ProductID'])
        if df.empty:
            df = pd.DataFrame(columns = ['period','CustomerID','GeographyID', 'gi_ptd_otc_gsv_vol'])
        else:
            df.loc[:,"ShipmentVolumn"] = df['ShipmentVolumn'].astype(float)
            df = df.groupby(['P_Freeze','CustomerNo','CustomerName','RegionName','ProvinceName_G','CitygroupName','CityName_G','CustomerID','GeographyID'],dropna=False)['ShipmentVolumn'].sum().reset_index()
            df.columns = ['period', 'customer_no', 'customer_name', 'mars_region_name', 'mars_province_name', 'mars_city_cluster_name', 'mars_city_name','CustomerID','GeographyID', 'gi_ptd_otc_gsv_vol']
            df = df[['period','CustomerID','GeographyID', 'gi_ptd_otc_gsv_vol']]
        return df
    def gi_ptd_otc_sellout_value(self):
        df = self.cmt_calculate_sellout_df[self.cmt_calculate_sellout_df['Period']==self.Period]
        df = pd.merge(df,self.cmt_segment_df[self.cmt_segment_df['SegmentName'].isin(['巧克力','箭牌'])],how='inner',on=['SegmentID'])
        df = pd.merge(df,self.cmt_customer_df[['CustomerID', 'CustomerNo', 'CustomerName']],how='left',on=['CustomerID'])
        df = pd.merge(df,self.cmt_geo_df[['GeographyID', 'RegionName', 'ProvinceName_G', 'CitygroupName','CityName_G']],how='left',on=['GeographyID'])
        df = pd.merge(df,self.cmt_subsegment_df[self.cmt_subsegment_df['SubSegmentName'].isin(['GUM'])][['SubSegmentID']],how='inner',on=['SubSegmentID'])
        df = pd.merge(df,self.cmt_product_df[self.cmt_product_df['CHINA_FORECAST_GROUP_DESC'].isin(self.OTC_list)][['ProductID']],how='inner',on=['ProductID'])
        if df.empty:
            df = pd.DataFrame(columns = ['period','CustomerID','GeographyID', 'gi_ptd_otc_sellout_value'])
        else:
            df.loc[:,"CALCULATE_GSV"] = df['CALCULATE_GSV'].astype(float)
            df = df.groupby(['Period','RegionName','ProvinceName_G','CitygroupName','CityName_G','CustomerNo','CustomerName','CustomerID','GeographyID'],dropna=False)['CALCULATE_GSV'].sum().reset_index()
            df.columns = ['period', 'mars_region_name', 'mars_province_name', 'mars_city_cluster_name', 'mars_city_name',
                'customer_no', 'customer_name','CustomerID','GeographyID', 'gi_ptd_otc_sellout_value']
            df = df[['period','CustomerID','GeographyID', 'gi_ptd_otc_sellout_value']]
        return df
    def gi_ptd_otc_sellout_vol(self):
        df = self.cmt_calculate_sellout_df[self.cmt_calculate_sellout_df['Period']==self.Period]
        df = pd.merge(df,self.cmt_segment_df[self.cmt_segment_df['SegmentName'].isin(['巧克力','箭牌'])],how='inner',on=['SegmentID'])
        df = pd.merge(df,self.cmt_customer_df[['CustomerID', 'CustomerNo', 'CustomerName']],how='left',on=['CustomerID'])
        df = pd.merge(df,self.cmt_geo_df[['GeographyID', 'RegionName', 'ProvinceName_G', 'CitygroupName','CityName_G']],how='left',on=['GeographyID'])
        df = pd.merge(df,self.cmt_subsegment_df[self.cmt_subsegment_df['SubSegmentName'].isin(['GUM'])][['SubSegmentID']],how='inner',on=['SubSegmentID'])
        df = pd.merge(df,self.cmt_product_df[self.cmt_product_df['CHINA_FORECAST_GROUP_DESC'].isin(self.OTC_list)][['ProductID']],how='inner',on=['ProductID'])
        if df.empty:
            df = pd.DataFrame(columns = ['period','CustomerID','GeographyID', 'gi_ptd_otc_sellout_vol'])
        else:
            df.loc[:,"CALCULATE_CASE"] = df['CALCULATE_CASE'].astype(float)
            df = df.groupby(['Period','RegionName','ProvinceName_G','CitygroupName','CityName_G','CustomerNo','CustomerName','CustomerID','GeographyID'],dropna=False)['CALCULATE_CASE'].sum().reset_index()
            df.columns = ['period', 'mars_region_name', 'mars_province_name', 'mars_city_cluster_name', 'mars_city_name',
                'customer_no', 'customer_name','CustomerID','GeographyID', 'gi_ptd_otc_sellout_vol']
            df = df[['period','CustomerID','GeographyID', 'gi_ptd_otc_sellout_vol']]
        return df
