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


class FixNotInTradeTime:
    def __init__(self):
        self.input_data_version = 'v10'
        self.input_data_folder_path = config.get_config('min_bar_develop_hist_folder_path')[self.input_data_version]
        self.need_fix_date_list = [20111012,20151202]
        self.need_fix_files_path_list = self.get_need_files_path_list()
        self.not_in_trade_time_detect_res = pd.read_pickle(
            'D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\detect_and_fix_outliers_data\\not_in_trade_time\detect_res.pkl')
        self.output_data_version = 'v11'
        self.output_data_folder_path = config.get_config('min_bar_develop_hist_folder_path')[self.output_data_version]
        
    def get_need_files_path_list(self):
        need_files_path_list = [os.path.join(self.input_data_folder_path,f'min{date_int}.pkl') for date_int in
                                self.need_fix_date_list]
        return need_files_path_list 


    def _get_empty_stk_df(self,stk_df):
        if stk_df.empty:
            return stk_df
    def get_need_fix_stk_list_by_res(self,df):
        idx = pd.IndexSlice
        need_fix_stk_list = []
        for stk in df.index.get_level_values(0).unique():
            stk_df = df.loc[idx[stk,:],:]
            tf_tmp = stk_df.loc[:,'open':'close']
            if tf_tmp.isna().all().all():
                need_fix_stk_list.append(stk)
        return need_fix_stk_list
    
    def fix_data(self,file_path):
        need_fix_data_df = pd.read_pickle(file_path)
        del_stk_list = self.get_need_fix_stk_list_by_res(need_fix_data_df)
        fixed_data_df = need_fix_data_df[~need_fix_data_df.index.get_level_values(0).isin(del_stk_list)]
        return fixed_data_df
        
    
    def fix_data_from_input_v10_data_folder_path(self):
        input_data_file_list = os.listdir(self.input_data_folder_path)
        for file_name in input_data_file_list:
            print('修正进度:{:.2f}%'.format((input_data_file_list.index(file_name)/len(input_data_file_list))*100))
            input_file_path = os.path.join(self.input_data_folder_path,file_name)
            output_save_path = os.path.join(self.output_data_folder_path,file_name)
            if input_file_path in self.need_fix_files_path_list:
                fixed_data_df = self.fix_data(input_file_path)
                fixed_data_df.to_pickle(output_save_path)
                print(f'{file_name}已修正处理')
            else:
                if os.path.exists(output_save_path):
                    print(f'!!!{file_name}!!!已存在')
                else:
                    shutil.copyfile(input_file_path,output_save_path)
                    # print(f'{file_name}无需修正处理')

if __name__ == '__main__':
    fix_not_in_trade_time_data = FixNotInTradeTime()

    fix_not_in_trade_time_data.fix_data_from_input_v10_data_folder_path()
