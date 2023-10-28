import os
import pickle
import sys

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\common")
from multiprocessing import Pool
from typing import List

import numpy as np
import pandas as pd

# sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject")
# from my_tools_packages import MyDecorator as MD
#
# running_time = MD.running_time
#
#
# import config
# import utilities as utl


class DetectPriceVolume:
    def __init__(self):
        self.data_version = 'v11'
        self.min_bar_folder_path = "F:\\local_tmp_data\\stock\\HK\\v11"
        # self.min_bar_folder_path = config.get_config('min_bar_develop_hist_folder_path')[self.data_version]
        self.min_bar_file_list = os.listdir(self.min_bar_folder_path)
        self.detected_res_fileds = ['file_name', 'data_num', 'correct_num', 'volume100_num', 'err_num']
        self.correct_threshold = 0.1

    def __get_volume_amount_0_and_equal_price_df(self, df):
        price_diff_close = df[['open', 'high', 'low', 'close']] - df[['close']].values
        equal_price_df = df[(price_diff_close == 0).all(axis=1)]
        volume_amount_0_and_price_is_correct_df = \
            equal_price_df[(equal_price_df.volume == 0) & (equal_price_df.amount == 0)]
        return volume_amount_0_and_price_is_correct_df

    def __get_volume_amount_0_and_price_nan_df(self, df):
        nan_df = df[df[['open', 'high', 'low', 'close']].isna().all(axis=1)]
        volume_amount_0_price_nan = nan_df[(nan_df.volume == 0) & (nan_df.amount == 0)]
        return volume_amount_0_price_nan

    def _get_correct_df(self, min_bar_df):
        df = min_bar_df.copy()
        del min_bar_df
        # 存在<0的数据，因此必须全部>0，才可以进一步选择正确的数据,而负数都出现在amount上，对于下面volume和amount为0，没有影响（彼此独立），所以可以concat
        df = df[df['amount'] >= 0]
        df['amount/volume'] = df['amount'] / df['volume']
        price_and_amount_volume_real_err = (df[['open', 'high', 'low', 'close']] / df[['amount/volume']].values - 1).abs()
        correct_threshold_df = df[(price_and_amount_volume_real_err <= self.correct_threshold).all(axis=1)]

        volume_amount_0_and_price_equal_df = self.__get_volume_amount_0_and_equal_price_df(df)
        volume_amount_0_price_nan_df = self.__get_volume_amount_0_and_price_nan_df(df)
        correct_df = pd.concat([correct_threshold_df, volume_amount_0_and_price_equal_df, volume_amount_0_price_nan_df])
        return correct_df

    def _get_100_volume_df(self, df):
        df['amount/volume100'] = df['amount'] / (df['volume'] * 100)
        price_and_amount_volume_real_err = (df[['open', 'high', 'low', 'close']] / df[['amount/volume100']].values - 1).abs()
        volume100_df = df[(price_and_amount_volume_real_err <= self.correct_threshold).all(axis=1)]
        fix_volume100_df = df[df.index.isin(volume100_df.index)]
        return fix_volume100_df

    def detect_single_file_data_num_info(self, file_name):
        print('get_file_data_info', file_name)
        file_path = os.path.join(self.min_bar_folder_path, file_name)
        min_bar_df = pd.read_pickle(file_path)
        correct_df = self._get_correct_df(min_bar_df)
        volume100_df = self._get_100_volume_df(min_bar_df)
        err_df = min_bar_df[~min_bar_df.index.isin(correct_df.index.union(volume100_df.index))]

        return file_name, len(min_bar_df), len(correct_df), len(volume100_df), len(err_df)

    def detect_all_files_data_num_info(self, process_num):
        with Pool(process_num) as pool:
            price_volume_err_df_idx_list = pool.map(self.detect_single_file_data_num_info, self.min_bar_file_list)
        # price_volume_err_df_idx_all_list = sum(price_volume_err_df_idx_list, [])
        with open("D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\detect_and_fix_outliers_data"
                  "\\price_all_volume_amount_err\\detect_and_fix_correct_threshold_01\\file_num_list.pkl", 'wb') as f:
            pickle.dump(price_volume_err_df_idx_list, f)
        # price_volume_err_df_idx_all_list

    def detect_singel_file_data_err_df_list(self, file_name):
        print('err_df', file_name)
        file_path = os.path.join(self.min_bar_folder_path, file_name)
        min_bar_df = pd.read_pickle(file_path)
        correct_df = self._get_correct_df(min_bar_df)
        volume100_df = self._get_100_volume_df(min_bar_df)
        err_df = min_bar_df[~min_bar_df.index.isin(correct_df.index.union(volume100_df.index))]
        return err_df

    def detect_all_files_data_err_df(self, process_num):
        with Pool(process_num) as pool:
            err_df_list = pool.map(self.detect_singel_file_data_err_df_list, self.min_bar_file_list)

        err_df_total = pd.concat(err_df_list)
        err_df_total.to_pickle(
            'D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy'
            '\\detect_and_fix_outliers_data\\price_all_volume_amount_err\\detect_threshold_01\\err_df_total.pkl')


if __name__ == "__main__":
    detect = DetectPriceVolume()
    detect.detect_all_files_data_num_info(16)
    # detect.detect_all_files_data_err_df(16)
    print('done')
