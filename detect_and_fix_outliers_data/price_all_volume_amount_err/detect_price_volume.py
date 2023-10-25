import os
import sys
from typing import List

import numpy as np
import pandas as pd

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject")
from my_tools_packages import MyDecorator as MD

running_time = MD.running_time

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\common")
import pickle
from multiprocessing import Pool

import config
import utilities as utl


class DetectPriceVolume:
    def __init__(self):
        self.data_version = 'v11'
        # self.min_bar_folder_path = "F:\\local_tmp_data\\stock\\HK\\v11"
        self.min_bar_folder_path = config.get_config('min_bar_develop_hist_folder_path')[self.data_version]
        self.min_bar_file_list = os.listdir(self.min_bar_folder_path)

        self.correct_threshold = 0.1
        self.volume100_threshold = 0.1
        self.volume_100_file_idx_dict = dict()

    def detect_df_100_volume_bool(self, df):
        df['amount/volume'] = df['amount'] / df['volume']
        price_and_amount_volume_real_err = (df[['open', 'high', 'low', 'close']] / df[['amount/volume']].values - 1).abs()
        volume100_df = df[(price_and_amount_volume_real_err <= (1 + self.volume100_threshold)).all(axis=1)
                          & (price_and_amount_volume_real_err >= (1 - self.volume100_threshold)).all(axis=1)]
        fix_volume100_df = df[df.index.isin(volume100_df.index)]
        return fix_volume100_df.empty

    def detect_file_100_volume(self, file_name):
        print('volume100:', file_name)
        file_path = os.path.join(self.min_bar_folder_path, file_name)
        min_bar_df = pd.read_pickle(file_path)
        if not self.detect_df_100_volume_bool(min_bar_df):
            return [file_name]
        else:
            return []

    def detect_all_files_100_volume(self, process_num):
        with Pool(process_num) as pool:
            volume100_df_idx_list = \
                pool.map(self.detect_file_100_volume, self.min_bar_file_list)
        volume100_df_idx_all_list = sum(volume100_df_idx_list, [])
        with open('volume100_df_idx_list.pkl', 'wb') as f:
            pickle.dump(volume100_df_idx_all_list, f)

    def _detect_df_price_volume_err(self, df):
        df['amount/volume'] = df['amount'] / df['volume']
        price_and_amount_volume_real_err = (df[['open', 'high', 'low', 'close']] / df[['amount/volume']].values - 1).abs()
        not_correct_df = df[(price_and_amount_volume_real_err > self.correct_threshold).all(axis=1)]
        not_volume100_df = df[(price_and_amount_volume_real_err > (1 + self.volume100_threshold)).all(axis=1)
                              & (price_and_amount_volume_real_err < (1 - self.volume100_threshold)).all(axis=1)]
        err_df = df[df.index.isin(not_correct_df.index.union(not_volume100_df.index))]
        return err_df

    def detect_single_file_data_price_volume_err(self, file_name):
        print('err_df', file_name)
        file_path = os.path.join(self.min_bar_folder_path, file_name)
        min_bar_df = pd.read_pickle(file_path)
        price_volume_err_df = self._detect_df_price_volume_err(min_bar_df)
        return price_volume_err_df.index.to_list()

    def detect_all_files_data_price_volume_err(self, process_num):
        with Pool(process_num) as pool:
            price_volume_err_df_idx_list = pool.map(self.detect_single_file_data_price_volume_err, self.min_bar_file_list)
        price_volume_err_df_idx_all_list = sum(price_volume_err_df_idx_list, [])
        with open("err_df_idx_list.pkl", 'wb') as f:
            pickle.dump(price_volume_err_df_idx_all_list, f)
        # price_volume_err_df_idx_all_list


if __name__ == "__main__":
    detect = DetectPriceVolume()
    detect.detect_all_files_100_volume(8)
    detect.detect_all_files_data_price_volume_err(8)
