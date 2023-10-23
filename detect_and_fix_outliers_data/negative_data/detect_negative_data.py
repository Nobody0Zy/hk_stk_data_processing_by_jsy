import os
import pickle
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


class DetectNegativeData:
    def __init__(self):
        self.min_bar_folder_path = config.get_config('min_bar_develop_hist_folder_path')['v20']
        # self.negative_data_files_list = pd.read_pickle("negative_data_files_list.pkl")

    def get_negative_data_files_list(self):
        negative_data_files_list = []
        min_bar_file_list = os.listdir(self.min_bar_folder_path)
        for file in min_bar_file_list:
            print(('进度: {:.2f}%').format((min_bar_file_list.index(file)/len(min_bar_file_list))*100))
            min_bar_df = pd.read_pickle(os.path.join(self.min_bar_folder_path,file))
            is_negative = min_bar_df<0
            if is_negative.any().any():
                negative_data_files_list.append(file)
        # with open('negative_data_files_list.pkl','wb') as f:
        #     pickle.dump(negative_data_files_list,f)
        return negative_data_files_list
    
    def get_negative_err_df(self):
        err_negative_df_list = []
        for file in self.negative_data_files_list:
            # 打印进度
            print('获取负数异常值')
            print(file)
            print('当前进度：', self.negative_data_files_list.index(file) + 1, '/', len(self.negative_data_files_list))
            min_bar_df = pd.read_pickle(os.path.join(self.min_bar_folder_path, file))
            # 获取负数的异常值
            err_negative_df = min_bar_df[(min_bar_df<0).any(axis=1)]
            err_negative_df_list.append(err_negative_df)
        err_negative_total_df = pd.concat(err_negative_df_list)
        err_negative_total_df.to_pickle('err_negative_df.pkl')
        err_negative_df.to_csv('err_negative_df.csv')
        return err_negative_total_df

if __name__ == "__main__":
    detect_negative_data = DetectNegativeData()
    negative_err_df= detect_negative_data.get_negative_data_files_list()