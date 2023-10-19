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


class DetectNanDataDate:
    def __init__(self) -> None:
        self.not_fillna_date_bar_path =\
            'D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\detect_and_fix_outliers_data\\nan_values\\compoose_date_bar_not_fillna\\date_bar.pkl'
        self.not_fillna_min_bar_path = "F:\\local_tmp_data\\stock\\HK\\v09"
        self.not_fillna_date_bar = pd.read_pickle(self.not_fillna_date_bar_path)
        self.open_is_nan_date_list = self.detect_col_isna_date('open')
        # 以下用于检测的列表是排除节日和天气之后，还需要检查的异常缺失
        self.open_is_nan_date_list_to_detect = [20120522, 20150619, 20151202, 20170411, 20170419]
        
        self.close_is_nan_date_list = self.detect_col_isna_date('close')
        # 以下用于检测的列表是排除节日和天气之后，还需要检查的缺失
        self.close_is_nan_date_list_to_detect = [20130927,]
        
    def detect_col_isna_date(self,col):
        idx = pd.IndexSlice
        col_all_nan_date_list = []
        date_list = self.not_fillna_date_bar.index.get_level_values(1).unique().to_list()
        for date in date_list:
            date_df = self.not_fillna_date_bar.loc[idx[:,date],:]
            if date_df[col].isna().all():
                col_all_nan_date_list.append(date)
        return col_all_nan_date_list
                
            
            
        
if __name__ == "__main__":
    detect_nan_date = DetectNanDataDate()
    open_is_nan_date = detect_nan_date.detect_col_isna_date('open')
    close_is_nan_date = detect_nan_date.detect_col_isna_date('close')