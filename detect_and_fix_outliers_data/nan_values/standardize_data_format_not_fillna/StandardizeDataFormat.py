import os
import sys
from typing import Dict, List, Set, Tuple, Union

import numpy as np
import pandas as pd
from pandarallel import pandarallel

# pandarallel.initialize()

sys.path.append("D:\QUANT_GAME\python_game\pythonProject")
from my_tools_packages import MyDecorator as MD

running_time = MD.running_time


sys.path.append("D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\common")

import utilities as utl

gen_stk_date_empty_df = utl.gen_stk_date_empty_df

class StandardizeDataFormat:
    def __init__(self,raw_idx_df):
        self.raw_idx_df = raw_idx_df
        self.std_df = None
        
    def gen_std_format_empty_df(self,trade_time_int_series_list):
        # 生成空的标准格式dataframe
        stk_idx = self.raw_idx_df.index.get_level_values('stk').unique()
        cols = self.raw_idx_df.columns
        std_format_empty_df = gen_stk_date_empty_df(stk_idx,trade_time_int_series_list,cols)
        self.std_df = std_format_empty_df
        
    
    def __get_stk_df_pre_post_price(self,stk_df,trade_time_key,trade_time_int):
        idx = pd.IndexSlice
        # 盘前，根据早市午市有所区别
        if 'open' in trade_time_key:
            if 'am' in trade_time_key:
                return stk_df.loc[idx[:, trade_time_int - 100:trade_time_int],:]
            elif 'pm' in trade_time_key:
                return stk_df.loc[idx[:, trade_time_int - 100 + 20:trade_time_int],:]
        if 'close' in trade_time_key:
            return stk_df.loc[idx[:, trade_time_int:trade_time_int + 10],:]
    
    def __gen_ohlcva_price_list_by_pre_post_price(self,pre_post_price_stk_df:pd.DataFrame) -> List[float]:
        ohlcva_price_list = [
            pre_post_price_stk_df.open.dropna().iloc[0],
            pre_post_price_stk_df.high.max(),
            pre_post_price_stk_df.low.min(),
            pre_post_price_stk_df.close.dropna().iloc[-1],
            pre_post_price_stk_df.volume.sum(),
            pre_post_price_stk_df.amount.sum()
        ]
        return ohlcva_price_list
       
    def _merge_stk_df_pre_post_price(self,stk_df,trade_session_dict):
        idx = pd.IndexSlice
        for trade_session_str_key, trade_time_int in trade_session_dict.items():
            pre_post_price_stk_df = self.__get_stk_df_pre_post_price(stk_df,trade_session_str_key,trade_time_int)
            if not pre_post_price_stk_df.isnull().all().all():
                ohlcva_price_by_pre_post_price_stk_df = self.__gen_ohlcva_price_list_by_pre_post_price(pre_post_price_stk_df)
                stk_df.loc[idx[:,trade_time_int],:] = ohlcva_price_by_pre_post_price_stk_df
        return stk_df
        
    def _concat_std_df_and_raw_idx_df_for_not_in_trade_session_idx(self):
        # 获取在交易时段的索引
        in_trade_session_idx = self.raw_idx_df.index.intersection(self.std_df.index)
        not_in_trade_session_idx = self.raw_idx_df.index.difference(self.std_df.index)
        # 获取在交易时段的dataframe
        std_df_df_with_in_trade_sesssion = self.std_df.copy()
        std_df_df_with_in_trade_sesssion.loc[in_trade_session_idx,:] = self.raw_idx_df.loc[in_trade_session_idx,:]
        
        not_in_trade_session_df = self.raw_idx_df.loc[not_in_trade_session_idx,:]
        # 将不在交易时段的dataframe拼接到标准格式的dataframe中
        concat_res_df = pd.concat([std_df_df_with_in_trade_sesssion,not_in_trade_session_df],axis=0)
        concat_res_df.sort_index(inplace=True)
        return concat_res_df
    
    def merge_pre_post_price_from_raw_idx_df(self,trade_session_date_time_int_dict):
        # 将不在交易时段的dataframe拼接到标准格式的dataframe中
        concat_std_df_and_raw_idx_df = self._concat_std_df_and_raw_idx_df_for_not_in_trade_session_idx()
        # 根据拼接非交易时段数据后的dataframe，将盘前盘后数据合并到开盘收盘时刻的价格中
        merge_pre_post_price_df = concat_std_df_and_raw_idx_df.groupby(
            level='stk',group_keys=False,).apply(
                self._merge_stk_df_pre_post_price,trade_session_dict=trade_session_date_time_int_dict)
        if merge_pre_post_price_df.index.nlevels == 3:
            merge_pre_post_price_df = merge_pre_post_price_df.reset_index(level=0,drop=True)
        elif merge_pre_post_price_df.index.nlevels == 1:
            raise ValueError('merge_pre_post_price_df.index.nlevels == 1,少了stk或date')
        return merge_pre_post_price_df
    
    
    
    def _fillna_for_std_stk_df(self,stk_df):
        stk_df.close.fillna(method='ffill',inplace=True) # 向后填充close,即用nan前一个close填充
        stk_df = stk_df.fillna(method='bfill',axis=1) # 用向后填充的close填充没有交易时刻的open,high,low
        stk_df.loc[:,['volume','amount']] = stk_df.loc[:,['volume','amount']].fillna(0)
        return stk_df
        
    # 把raw_df 填入标准格式的dataframe中
    def fill_in_standard_format_df(self,merge_res_df_by_pre_post_price,fillna=False):
        # 将原始数据填入标准格式的dataframe中
        in_trade_session_idx = self.std_df.index.intersection(merge_res_df_by_pre_post_price.index)
        fill_res_std_df = self.std_df.copy()
        fill_res_std_df.loc[in_trade_session_idx,:] = merge_res_df_by_pre_post_price.loc[in_trade_session_idx,:]
        fill_res_std_df.sort_index(inplace=True)
        if fillna:
        # 填充nan
            fill_res_std_df = fill_res_std_df.groupby(
                level='stk',group_keys=False).apply(
                    self._fillna_for_std_stk_df)
        return fill_res_std_df
    
    
    