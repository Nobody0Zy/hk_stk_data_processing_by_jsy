import os
import re
import sys
from datetime import datetime, timedelta
from multiprocessing import Pool
from typing import Dict, List, Set, Tuple, Union

import numpy as np
import pandas as pd
from pandarallel import pandarallel
from tqdm import tqdm

sys.path.append("D:\QUANT_GAME\python_game\pythonProject")
from my_tools_packages import MyDecorator as MD

running_time = MD.running_time

sys.path.append("D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\common")
import config
import utilities as utl

gen_stk_date_empty_df = utl.gen_stk_date_empty_df
get_trade_session_time_tuple = utl.get_trade_session_time_tuple
gen_trade_session_date_time_dict = utl.gen_trade_session_date_time_dict
    
import StandardizeDataFormat as StdFormat


def set_index_for_raw_df(df):
    df = df.copy()
    df.drop(columns=['date','time'],inplace=True)
    
    # 将date_time列转换为int类型，并修改字符串格式
    df['date_time'] = \
        df['date_time'].str.replace(r'[- :]', '',regex=True).str[:-2].astype(np.int64)

    # 设置“stk”和“date_time”为索引
    df.set_index(['stk', 'date_time'], inplace=True)
    
    # ！！！原始数时间轴可能存在问题，需要对索引重新排序
    df.sort_index(inplace=True)
    # 去除重复索引
    df = df[~df.index.duplicated(keep='first')]
    
    return df


def gen_trade_time_int_series_list(trade_session_date_time_dict):
    trade_time_series_am = pd.date_range(
        start=trade_session_date_time_dict['am_open_time'],
        end=trade_session_date_time_dict['am_close_time'],
        freq='1min')
    trade_time_series_pm = pd.date_range(
        start=trade_session_date_time_dict['pm_open_time'],
        end=trade_session_date_time_dict['pm_close_time'],
        freq='1min')
    time_series = trade_time_series_am.append(trade_time_series_pm)
    # 将时间序列中的字符串转换为int，并排序
    # 格式: 202101010931
    time_series = time_series.strftime('%Y%m%d%H%M').astype(np.int64).sort_values()
    return time_series

def process_min_bar_of_single_file(raw_pkl_file_path,save_folder_path):
    """
    处理单个文件的分钟线数据
    param: raw_pkl_file_path: 原始数据文件路径
    param: save_floder_path: 保存的文件夹路径
    
    """
    raw_df = pd.read_pickle(raw_pkl_file_path)
    file_name = os.path.basename(raw_pkl_file_path)
    print(f"file_name: {file_name}")
    # 为原始数据添加索引
    raw_idx_df = set_index_for_raw_df(raw_df)
    
    # -----------------------------------------------------------------------
    # 获取对应日期的交易时间段，例如((09:31, 12:00), (13:31, 16:00))
    trade_session_time_tuple = get_trade_session_time_tuple(file_name)  
    # 生成早市午市开盘时间和收盘时间 
    # {'am_open_time': '2021-01-01 09:31', 'am_close_time': '2021-01-01 12:00', 
    # 'pm_open_time': '2021-01-01 13:31', 'pm_close_time': '2021-01-01 16:00'}
    trade_session_date_time_str_dict = gen_trade_session_date_time_dict(file_name,trade_session_time_tuple)
    # 生成当日交易时间序列列表
    trade_time_int_series_list = gen_trade_time_int_series_list(trade_session_date_time_str_dict)

    
    # -----------------------------------------------------------------------
    std_format = StdFormat.StandardizeDataFormat(raw_idx_df)
    # 生成标准格式的空dataframe
    std_format.gen_std_format_empty_df(trade_time_int_series_list)


    #----------------------------------------------------------------
    trade_session_date_time_int_dict = gen_trade_session_date_time_dict(file_name,trade_session_time_tuple,return_int=True)
    # 合并原始数据的盘前盘后数据
    merge_res_df_by_pre_post_price = \
        std_format.merge_pre_post_price_from_raw_idx_df(trade_session_date_time_int_dict)
        
    # -----------------------------------------------------------------------
    # 把df_by_merge_pre_post_price 填入标准格式的dataframe中
    fill_res_df_by_merge_res_df_to_std_df = \
        std_format.fill_in_standard_format_df(merge_res_df_by_pre_post_price)

    # -----------------------------------------------------------------------
    
    save_file_name = f"min{file_name.split('.')[0].replace('-','')}.pkl"
    save_fill_res_df_file_path = os.path.join(save_folder_path,save_file_name)
    fill_res_df_by_merge_res_df_to_std_df.to_pickle(save_fill_res_df_file_path)


def process_min_bar_in_files_path(split_files_path_list):
    save_folder_path = config.get_config('min_bar_develop_hist_folder_path')['v10']
    # 处理文件路径列表中的文件
    for file_path in split_files_path_list:
        process_min_bar_of_single_file(file_path,save_folder_path)
    return True


def multiprocess_process_hk_min_bar(jsy_raw_data_date_pkl_file_list ,process_num):
    
    mp_process_files_path_list = []
    # 将文件路径列表按照进程数进行切分(等分)
    for i in range(process_num):
        mp_process_files_path_list.append(jsy_raw_data_date_pkl_file_list[i::process_num])
        
    # 多进程处理
    pool = Pool(process_num)
    results = pool.imap(process_min_bar_in_files_path, mp_process_files_path_list)
    pool.close()
    pool.join()

@running_time
def main():
    print('The game is on!')
    v02_floder_path = config.get_config('min_bar_develop_hist_folder_path')['v02']
    # save_folder_path_v10 = config.get_config('min_bar_develop_hist_folder_path')['v10']
    need_standardize_files_list = []
    for root, dirs, files in os.walk(v02_floder_path):
        if not dirs:
            if 'other' not in root:
                need_standardize_files_path = [os.path.join(root,file) for file in files]
                need_standardize_files_list.extend(need_standardize_files_path)
    # need_standardize_files_sum_list = sum(need_standardize_files_list,[])
    need_standardize_files_list.sort()
    # need_standardize_files_sum_list = need_standardize_files_sum_list[:100]
    
    multiprocess_process_hk_min_bar(need_standardize_files_list, process_num=10)
                

if __name__ == "__main__":
    main()