from typing import Tuple

import config
import numpy as np
import pandas as pd

global trade_session_change_date_dict
trade_session_change_date_dict = config.get_config('trade_session_change_date')


def gen_stk_date_empty_df(stk_list,trade_date_time_list,columns_list):
    if not stk_list.any() or not trade_date_time_list.any() or not columns_list.any():
        raise ValueError("stk_list or date_time_list or columns_list is empty")
    multi_idx = pd.MultiIndex.from_product([stk_list,trade_date_time_list],names=['stk','date_time'])
    empty_df = pd.DataFrame(index=multi_idx,columns=columns_list)
    empty_df.sort_index(inplace=True)
    return empty_df

def get_trade_session_time_tuple(file_name) -> Tuple[str,str]:
    trade_session_change_date_dict_keys_list = list(trade_session_change_date_dict.keys())

    if len(trade_session_change_date_dict_keys_list) != 3:
        raise ValueError("trade_session_change_date_dict_keys_list != 3")
    
    # 为了防止之后循环中出现文件名不在字典内的情况，添加一个最大值
    # 避免out of range
    trade_session_change_date_dict_keys_list.append('9999-99-99.pkl')

    file_date_str = file_name.split('.')[0]
    # 根据file_name内的日期，获取对应的交易时间段
    for n in range(0,3):
        if trade_session_change_date_dict_keys_list[n] <= file_date_str <= trade_session_change_date_dict_keys_list[n+1]:
            return trade_session_change_date_dict[trade_session_change_date_dict_keys_list[n]]

def gen_trade_session_date_time_dict(file_name,trade_session_time_tuple,output_int=False):
    
    date_str = file_name.split('.')[0]
    trade_session_date_time_dict = {
        'am_open_time': date_str + ' ' + trade_session_time_tuple[0][0],
        'am_close_time': date_str + ' ' + trade_session_time_tuple[0][1],
        'pm_open_time': date_str + ' ' +trade_session_time_tuple[1][0],
        'pm_close_time': date_str + ' ' + trade_session_time_tuple[1][1],
    }
    if output_int:
        for key in trade_session_date_time_dict.keys():
            trade_session_date_time_dict[key] = \
                trade_session_date_time_dict[key].replace(r'[- :]', '',regex=True).str[:-2].astype(np.int64)
        return trade_session_date_time_dict
    else:
        return trade_session_date_time_dict

