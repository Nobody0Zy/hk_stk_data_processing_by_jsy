import os
import sys

import numpy as np
import pandas as pd

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject")
from multiprocessing import Pool

from my_tools_packages import MyDecorator as MD

running_time = MD.running_time

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\common")

import config
import utilities as utl


class DetectNotInTradeTime:
    def __init__(self):
        self.input_data_version = 'v02'
        self.input_data_folder_path = config.get_config('min_bar_develop_hist_folder_path')[self.input_data_version]
        self.need_detect_files_path_list = self.get_detect_files_path_list()
        self._get_trade_session_time_tuple = utl.get_trade_session_time_tuple
        self._gen_trade_session_date_time_dict = utl.gen_trade_session_date_time_dict

    def set_index_for_raw_df(self, df):
        df = df.copy()
        df.drop(columns=['date', 'time'], inplace=True)

        # 将date_time列转换为int类型，并修改字符串格式
        df['date_time'] = \
            df['date_time'].str.replace(r'[- :]', '', regex=True).str[:-2].astype(np.int64)

        # 设置“stk”和“date_time”为索引
        df.set_index(['stk', 'date_time'], inplace=True)

        # ！！！原始数时间轴可能存在问题，需要对索引重新排序
        df.sort_index(inplace=True)
        # 去除重复索引
        df = df[~df.index.duplicated(keep='first')]

        return df

    def get_detect_files_path_list(self):
        need_detect_files_path_list = []
        for root, dirs, files in os.walk(self.input_data_folder_path):
            if not dirs:
                if 'other' not in root:
                    need_detect_files_paths = [os.path.join(root, file) for file in files]
                    need_detect_files_path_list.extend(need_detect_files_paths)
        need_detect_files_path_list.sort()
        return need_detect_files_path_list

    def _get_trade_session_start_end_dict(self, trade_session_date_time_dict):
        for trade_session_str_key, trade_time_int in trade_session_date_time_dict.items():
            if 'open' in trade_session_str_key:
                if 'am' in trade_session_str_key:
                    trade_session_date_time_dict[trade_session_str_key] = trade_time_int - 100
                if 'pm' in trade_session_str_key:
                    trade_session_date_time_dict[trade_session_str_key] = trade_time_int - 100 + 20
            if 'close' in trade_session_str_key:
                trade_session_date_time_dict[trade_session_str_key] = trade_time_int + 10
        return trade_session_date_time_dict

    def get_trade_session_idx(self, file_idx_df, trade_session_date_time_dict):
        idx = pd.IndexSlice
        trade_session_start_end_dict = self._get_trade_session_start_end_dict(trade_session_date_time_dict)
        am_file_idx_df = \
            file_idx_df.loc[idx[:, trade_session_start_end_dict['am_open_time']:trade_session_date_time_dict['am_close_time']], :]
        pm_file_idx_df = \
            file_idx_df.loc[idx[:, trade_session_start_end_dict['pm_open_time']:trade_session_start_end_dict['pm_close_time']], :]
        trade_session_file_df = pd.concat([am_file_idx_df, pm_file_idx_df])
        # trade_session_idx = trade_session_file_df.index
        return trade_session_file_df

    def detect_single_file(self, file_path):
        # 获取对应的收盘开盘时间
        file_name = os.path.basename(file_path)
        print(file_name)
        trade_session_time_tuple = self._get_trade_session_time_tuple(file_name)
        trade_session_date_time_dict = self._gen_trade_session_date_time_dict(file_name, trade_session_time_tuple, return_int=True)
        file_df = pd.read_pickle(file_path)
        file_idx_df = self.set_index_for_raw_df(file_df)
        trade_session_file_df = self.get_trade_session_idx(file_idx_df, trade_session_date_time_dict)
        not_in_trade_session_df = file_idx_df[~file_idx_df.index.isin(trade_session_file_df.index)]
        if not not_in_trade_session_df.empty:
            print(file_name,'-----------------------detect!-----------------------------------')
            return not_in_trade_session_df

    def detect_data_not_int_trade_time(self):
        not_in_trade_time_df_list = []
        for file_path in self.need_detect_files_path_list:
            print('进度:{:.2f}%'.format(
                  self.need_detect_files_path_list.index(file_path)/len(self.need_detect_files_path_list)))
            not_in_trade_time_df = self.detect_single_file(file_path)
            not_in_trade_time_df_list.append(not_in_trade_time_df)
        not_in_trade_time_df_total_res = pd.concat(not_in_trade_time_df_list)
        return not_in_trade_time_df_total_res

    def detect_data_not_int_trade_time_by_multiprocessing(self,process_num):
        with Pool(process_num) as pool:
            not_in_trade_time_df_list = pool.map(self.detect_single_file,self.need_detect_files_path_list)
        not_in_trade_time_df_total_res = pd.concat(not_in_trade_time_df_list)
        not_in_trade_time_df_total_res.insert(
            1,'date',not_in_trade_time_df_total_res.index.get_level_values(1)//10000)
        not_in_trade_time_df_total_res.sort_values(by='date',inplace=True)
        not_in_trade_time_df_total_res.to_pickle(
            'D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\detect_and_fix_outliers_data\\not_in_trade_time\\detect_res.pkl')
        return not_in_trade_time_df_total_res

    
if __name__ == '__main__':
    detect = DetectNotInTradeTime()

    not_in_trade_time_df_total_res = detect.detect_data_not_int_trade_time_by_multiprocessing(12)
