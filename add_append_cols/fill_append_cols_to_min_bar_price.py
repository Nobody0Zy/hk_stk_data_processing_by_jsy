# -*- coding: utf-8 -*-
import datetime as dt
import os
import sys

import numpy as np
import pandas as pd

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject")
from my_tools_packages import MyDecorator as MD

running_time = MD.running_time

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\common")
from multiprocessing import Pool

import config
import utilities as utl

idx = pd.IndexSlice


class FillAppnedCols:
    def __init__(self):
        self.data_version = 'v20'
        self.min_bar_price_folder_path = config.get_config('min_bar_develop_hist_folder_path')[self.data_version]
        # self.min_bar_price_folder_path = "F:\\local_tmp_data\\stock\\HK\\v20_min_bar\\price"
        self.min_bar_file_list = os.listdir(self.min_bar_price_folder_path)
        # self.min_bar_append_cols_folder_path = "F:\\local_tmp_data\\stock\\HK\\v20_min_bar\\apd_cols"
        self.added_res_min_bar_folder_path = \
            "D:\\QUANT_GAME\\python_game\\pythonProject\\DATA\\local_develop_data\\stock\\HK_stock_data\\jsy_develop_concat_price_apd_cols_data\\min_bar_v20"

    @running_time
    def add_append_cols_to_min_bar_price(self):
        for min_bar_file_name in self.min_bar_file_list:
            print(min_bar_file_name)
            # input_file_path
            price_df_path = os.path.join(self.min_bar_price_folder_path, min_bar_file_name)
            apd_df_path = os.path.join(self.min_bar_append_cols_folder_path, min_bar_file_name)
            # output_file_path
            save_file_path = os.path.join(self.added_res_min_bar_folder_path, min_bar_file_name)
            # -----
            price_df = pd.read_pickle(price_df_path)
            apd_df = pd.read_pickle(apd_df_path)
            addred_df = pd.concat([price_df, apd_df], axis=1)
            # save
            addred_df.to_pickle(save_file_path)


if __name__ == '__main__':
    print('New Game')
    fill = FillAppnedCols()
    fill.add_append_cols_to_min_bar_price()
