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


class FixNegativeData:
    def __init__(self) -> None:
        self.input_data_version = 'v11'
        self.input_data_folder_path = config.get_config('min_bar_develop_hist_folder_path')[self.input_data_version]
        self.negative_data_files_list = pd.read_pickle("negative_data_files_list.pkl")
        self.output_data_version = 'v20'
        self.output_data_folder_path = config.get_config('min_bar_develop_hist_folder_path')[self.output_data_version]
    
    def fix_data(self,file_path):
        min_bar_df = pd.read_pickle(file_path)
        is_negative_df = min_bar_df[(min_bar_df<0).any(axis=1)]
        is_negative_volume0_df = is_negative_df[is_negative_df.volume==0]
        is_negative_volume_not0_df = is_negative_df[is_negative_df.volume!=0]
        min_bar_df.loc[is_negative_volume0_df.index,'amount'] = 0
        min_bar_df.loc[is_negative_volume_not0_df.index,'amount'] =\
            min_bar_df.loc[is_negative_volume_not0_df.index,'close']*min_bar_df.loc[is_negative_volume_not0_df.index,'volume']
        return min_bar_df
        
    def fix_data_from_input_v11_data_folder_path(self):
        input_data_file_list = os.listdir(self.input_data_folder_path)
        for file_name in input_data_file_list:
            print('修正进度:{:.2f}%'.format((input_data_file_list.index(file_name) / len(input_data_file_list)) * 100))
            input_file_path = os.path.join(self.input_data_folder_path, file_name)
            output_save_path = os.path.join(self.output_data_folder_path, file_name)
            if file_name in self.negative_data_files_list:
                fixed_data_df = self.fix_data(input_file_path)
                fixed_data_df.to_pickle(output_save_path)
                print(f'{file_name}已修正处理')
            else:
                if os.path.exists(output_save_path):
                    print(f'!!!{file_name}!!!已存在')
                else:
                    shutil.copyfile(input_file_path, output_save_path)
                    # print(f'{file_name}无需修正处理')
        
            
            
        
if __name__ == '__main__':
    fix_negative_data = FixNegativeData()
    fix_negative_data.fix_data_from_input_v11_data_folder_path()