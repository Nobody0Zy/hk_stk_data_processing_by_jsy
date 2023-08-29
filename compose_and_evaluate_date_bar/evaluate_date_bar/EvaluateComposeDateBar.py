import os
import sys

import numpy as np
import pandas as pd

sys.path.append("D:\\QUANT_GAME\\python_game\\pythonProject")
from my_tools_packages import MyDecorator as MD

running_time = MD.running_time

sys.path.append("D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\common")
import config
import utilities as utl

EvalDateBar = utl.EvalDateBar

class EvaluateComposeDateBar:
    def __init__(self,evaluate_date_bar,other_source_date_bar,evaluated_date_bar_info,other_source_date_bar_info):
        self.evaluated_date_bar_version = evaluated_date_bar_info
        self.other_source_date_bar_version = other_source_date_bar_info
        self.evaluate_date_bar = EvalDateBar.EvaluateDateBar(evaluate_date_bar,other_source_date_bar)
    
    @running_time
    def get_relative_err_percent(self,threshold):
        return self.evaluate_date_bar.calc_err_percent(threshold)
    
    @running_time
    def plot_date_bar(self,stk,start_date,end_date,save_fig=False,save_png_folder_path=None):
        evaluated_df_ylabel = self.evaluated_date_bar_version + '_price'
        other_source_df_ylabel = self.other_source_date_bar_version + '_rice'
        file_name = stk + '_' + str(start_date) + '_' + str(end_date)+'_vs_'+self.other_source_date_bar_version + '.png'
        save_png_file_path = os.path.join(save_png_folder_path,self.evaluated_date_bar_version,file_name)
        plot_info = {
            'stk': stk,
            'start_date': start_date,
            'end_date': end_date,
            'title':file_name,
            'evaluated_df_ylabel': evaluated_df_ylabel,
            'other_source_df_ylabel': other_source_df_ylabel,
            'save_fig': save_fig,
            'save_png_file_path': save_png_file_path
            
        }
        self.evaluate_date_bar.plot_date_bar(**plot_info)
    