# -*- coding: utf-8 -*-

import os
import shutil
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

running_time = MD.running_time


class FixMinBarThresholdNot01Data:
    def __init__(self, min_bar_df, correct_threshold):
        self.min_bar_df = min_bar_df
        self.correct_threshold = correct_threshold

    def fix_100_volume(self):
        df = self.min_bar_df.copy()
        df['amount/volume100'] = df['amount'] / (df['volume'] * 100)
        price_and_amount_volume_real_err = (df[['open', 'high', 'low', 'close']] / df[['amount/volume100']].values - 1).abs()
        volume100_df = df[(price_and_amount_volume_real_err <= self.correct_threshold).all(axis=1)]

        need_fix_idx = volume100_df.index
        if len(need_fix_idx) != 0:
            self.min_bar_df.loc[need_fix_idx, 'volume'] = df.loc[need_fix_idx, 'volume'] * 100
            # print('err:', df.loc[need_fix_idx, :])
            # print('fixed:', self.min_bar_df.loc[need_fix_idx, :])

    def fix_volume_0_amount_not_0_price_equal(self):
        # volume == 0，amount != 0 ，price_equal == True
        # 数据量：724656(0.0453 %)
        # 可令amount == 0 来修复
        df = self.min_bar_df.copy()
        price_equal_df = df[(df[['open', 'high', 'low', 'close']] - df[['open']].values == 0).all(axis=1)]
        volume_0_amount_not_0_df = df[(df.volume == 0) & (df.amount != 0)]

        need_fix_idx = price_equal_df.index.intersection(volume_0_amount_not_0_df.index, sort=None)

        if len(need_fix_idx) != 0:
            self.min_bar_df.loc[need_fix_idx, 'amount'] = 0
            # print('err:', df.loc[need_fix_idx, :])
            # print('fixed:', self.min_bar_df.loc[need_fix_idx, :])

    def fix_volume_not_0_amount_0_price_not_equal(self):
        # volume != 0, amount == 0, price_equal == False
        # 数据量 6175
        # 可对price_err < 0.5的数据进行修复
        # price_err = abs(price.all() / price.open - 1)
        # 用(high + low) / 2 * volume来修复amount
        # 可全部修复 6175
        df = self.min_bar_df.copy()
        price_err = (df[['open', 'high', 'low', 'close']] / df[['open']].values - 1).abs()
        price_err_05_df = df[(price_err <= 0.5).all(axis=1)]
        volume_not_0_amount_0_df = df[(df.volume != 0) & (df.amount == 0)]

        need_fix_idx = price_err_05_df.index.intersection(volume_not_0_amount_0_df.index, sort=None)

        if len(need_fix_idx) != 0:
            self.min_bar_df.loc[need_fix_idx, 'amount'] = \
                ((df.loc[need_fix_idx, 'high'] + df.loc[need_fix_idx, 'low']) / 2) * df.loc[need_fix_idx, 'volume']
            # print('err', df.loc[need_fix_idx, :])
            # print('fixed', self.min_bar_df.loc[need_fix_idx, :])

    def fix_amount_negative(self):
        # 数据量：1815
        # volume == 0, amount < 0
        # volume == 0, price_equal True 可令amount == 0 来修复
        # volume != 0, price_equal False, price_err < 0.5,可用 (high+low)/2
        df = self.min_bar_df.copy()
        df = df[(df < 0).any(axis=1)]
        if not df.empty:

            price_equal_df = df[(df[['open', 'high', 'low', 'close']] - df[['open']].values == 0).all(axis=1)]
            volume_0_amount_not_0_df = df[(df.volume == 0) & (df.amount != 0)]

            need_fix_amount_0_idx = price_equal_df.index.intersection(volume_0_amount_not_0_df.index, sort=None)

            if len(need_fix_amount_0_idx) != 0:
                self.min_bar_df.loc[need_fix_amount_0_idx, 'amount'] = 0

                # print('err:', df.loc[need_fix_amount_0_idx, :])
                # print('fixed:', self.min_bar_df.loc[need_fix_amount_0_idx, :])

            price_err = (df[['open', 'high', 'low', 'close']] / df[['open']].values - 1).abs()
            price_err_05_df = df[(price_err <= 0.5).all(axis=1)]
            volume_not_0_amount_negative_df = df[df.amount < 0]

            need_fix_amount_price_idx = price_err_05_df.index.intersection(volume_not_0_amount_negative_df.index, sort=None)

            if len(need_fix_amount_price_idx) != 0:
                self.min_bar_df.loc[need_fix_amount_price_idx, 'amount'] = \
                    (((df.loc[need_fix_amount_price_idx, 'high'] + df.loc[need_fix_amount_price_idx, 'low']) / 2) *
                     df.loc[need_fix_amount_price_idx, 'volume'])

                # print('err:', df.loc[need_fix_amount_price_idx, :])
                # print('fixed', self.min_bar_df.loc[need_fix_amount_price_idx, :])

    def fix_min_bar_df(self):
        self.fix_100_volume()
        self.fix_volume_0_amount_not_0_price_equal()
        self.fix_volume_not_0_amount_0_price_not_equal()
        self.fix_amount_negative()
        return self.min_bar_df


class FixThresholdNot01Data:
    def __init__(self):
        self.input_data_version = 'v11'
        # self.input_data_folder_path = config.get_config('min_bar_develop_hist_folder_path')[self.input_data_version]
        self.input_data_folder_path = "F:\\local_tmp_data\\stock\\HK\\v11"
        self.detected_res_list = pd.read_pickle('D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy'
                                                '\\detect_and_fix_outliers_data\\price_all_volume_amount_err\\file_num_list.pkl')
        self.need_fix_threshold_not_01_data_file_list = [res[0] for res in self.detected_res_list if res[4] != 0]
        self.correct_threshold = 0.5
        self.output_data_version = 'v20'
        # self.output_data_folder_path = config.get_config('min_bar_develop_hist_folder_path')[self.output_data_version]
        self.output_data_tmp_folder_path = 'F:\\local_tmp_data\\stock\\HK\\v20'

    def fix_threshold_not_01_data_from_folder_path(self):
        input_file_list = os.listdir(self.input_data_folder_path)
        for file_name in input_file_list:
            print('修正进度{:.2f}%'.format(
                (input_file_list.index(file_name) / len(input_file_list)) * 100
            ))
            input_file_path = os.path.join(self.input_data_folder_path, file_name)
            output_save_path = os.path.join(self.output_data_tmp_folder_path, file_name)
            if file_name in self.need_fix_threshold_not_01_data_file_list:
                min_bar_df = pd.read_pickle(input_file_path)
                fix_min_bar_threshold_not_01_data = FixMinBarThresholdNot01Data(min_bar_df, self.correct_threshold)
                fixed_df = fix_min_bar_threshold_not_01_data.fix_min_bar_df()
                fixed_df.to_pickle(output_save_path)
            else:
                if os.path.exists(output_save_path):
                    print(f'!!!{file_name}!!!已存在')
                else:
                    shutil.copyfile(input_file_path, output_save_path)

    def fix_threshold_not_01_data_from_folder_path_by_single(self, file_list):
        for file_name in file_list:
            input_file_path = os.path.join(self.input_data_folder_path, file_name)
            output_save_path = os.path.join(self.output_data_tmp_folder_path, file_name)
            if file_name in self.need_fix_threshold_not_01_data_file_list:
                min_bar_df = pd.read_pickle(input_file_path)
                fix_min_bar_threshold_not_01_data = FixMinBarThresholdNot01Data(min_bar_df, self.correct_threshold)
                fixed_df = fix_min_bar_threshold_not_01_data.fix_min_bar_df()
                fixed_df.to_pickle(output_save_path)
            else:
                if os.path.exists(output_save_path):
                    print(f'!!!{file_name}!!!已存在')
                else:
                    shutil.copyfile(input_file_path, output_save_path)

    def fix_threshold_not_01_data_from_fodler_path_by_multiprocessing(self, process_num):
        input_file_list = os.listdir(self.input_data_folder_path)
        mp_process_files_path_list = []
        # 将文件路径列表按照进程数进行切分(等分)
        for i in range(process_num):
            mp_process_files_path_list.append(input_file_list[i::process_num])

        # 多进程处理
        pool = Pool(process_num)
        results = pool.imap(self.fix_threshold_not_01_data_from_folder_path_by_single, mp_process_files_path_list)
        pool.close()
        pool.join()


if __name__ == '__main__':
    print('New Game')
    fix_from_folder = FixThresholdNot01Data()
    # fix_from_folder.fix_threshold_not_01_data_from_folder_path()
    fix_from_folder.fix_threshold_not_01_data_from_fodler_path_by_multiprocessing(16)
