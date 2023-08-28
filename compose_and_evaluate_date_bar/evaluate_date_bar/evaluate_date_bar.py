import os
import sys
from typing import Dict, List

import numpy as np
import pandas as pd

sys.path.append("D:\\QUANT_GAME\\python_game\\pythonProject")
from my_tools_packages import MyDecorator as MD

running_time = MD.running_time

sys.path.append("D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\common")
import config
import utilities as utl

hk_date_bar_em_file_path = config.get_config('date_bar_other_source_file_path')['eastmoney_nfq_by_ak']
hk_date_bar_sina_file_path = config.get_config('date_bar_other_source_file_path')['sina_nfq_by_ak']

sys.path.append("D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\compose_and_evaluate_date_bar\\evaluate_date_bar")
import EvaluateComposeDateBar

EvalDateBar = EvaluateComposeDateBar.EvaluateComposeDateBar


# ========================================================================== 
def save_err_percent_to_csv(err_percent_df_list,save_file_path):
    err_percent_total_df =  pd.concat(err_percent_df_list,axis=1)
    err_percent_total_df.columns = ['err_percent_compose_em','err_percent_compose_sina']
    err_percent_total_df.to_csv(save_file_path)
    
# ==========================================================================
def main():
    # 获取需要评估的数据
    hk_date_bar_em = pd.read_pickle(hk_date_bar_em_file_path)
    hk_date_bar_sina = pd.read_pickle(hk_date_bar_sina_file_path)
    # 由分钟线合成的日线数据
    hk_date_bar_version = 'v10'
    hk_date_bar_file_path = config.get_config('date_bar_develop_hist_file_path')[hk_date_bar_version]
    hk_date_bar_by_compose = pd.read_pickle(hk_date_bar_file_path)
    
    # ==============================================================================
    threshold = 0.05
    # 评估compose和em的差异
    eval_compose_em = EvalDateBar(hk_date_bar_by_compose,hk_date_bar_em,hk_date_bar_version,'em')
    eval_compose_em_err_percent =  eval_compose_em.get_relative_err_percent(threshold)
    # 评估compose和sina的差异
    eval_compose_sina = EvalDateBar(hk_date_bar_by_compose,hk_date_bar_sina,hk_date_bar_version,'sina')
    eval_compose_sina_err_percent = eval_compose_sina.get_relative_err_percent(threshold)
    # 合并评估结果，并保存输出
    err_percent_df_list = [eval_compose_em_err_percent,eval_compose_sina_err_percent]
    save_folder_path = config.get_config('evaluate_res_folder_path')['res_df']
    res_file_name = hk_date_bar_version + '_err_percent.csv'
    save_file_path = os.path.join(save_folder_path,res_file_name)
    save_err_percent_to_csv(err_percent_df_list,save_file_path)
    
    # ==============================================================================
    # 绘制图并保存
    # 绘制的股票和日期范围
    save_folder_path = config.get_config('evaluate_res_folder_path')['res_plot']
    plot_info = {
            'stk': 'hk00001',
            'start_date': 20190101,
            'end_date': 20201231,
            'save_fig': True,
            'save_png_folder_path': save_folder_path,
     }
    
    # 绘制compose和em的日线图，比较差异，并保存
    eval_compose_em.plot_date_bar(**plot_info)
    # 绘制compose和sina的日线图，比较差异,并保存
    eval_compose_sina.plot_date_bar(**plot_info)

if __name__ == '__main__':
    main()
    