
import datetime
import math
import os
import sys

# import mplfinance as mpf
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

EvalDateBar = utl.EvalDateBar

@running_time
def evaluate_date_bar_by_rela_err(eval_df1,eval_df2):
    """
    通过相对的差异评估两个DataFrame的差异
    """
    eval_date_bar = EvalDateBar.EvaluateDateBar(eval_df1,eval_df2)
    err_percent = eval_date_bar.calc_err_percent()
    return err_percent

def evaluate_date_bar_by_plot_date_bar(eval_df1,eval_df2,stk,start_date,end_date,evaluate_df_ylabel,other_source_df_ylabel):
    eval_date_bar = EvalDateBar.EvaluateDateBar(eval_df1,eval_df2)
    eval_date_bar.plot_date_bar(stk,start_date,end_date,evaluate_df_ylabel,other_source_df_ylabel)
    # print()
def main():
    # 用于参考比较评估的数据
    hk_date_bar_em = pd.read_pickle(hk_date_bar_em_file_path)
    hk_date_bar_sina = pd.read_pickle(hk_date_bar_sina_file_path)
    # 由分钟线合成的日线数据
    hk_date_bar_version = 'v10'
    hk_date_bar_file_path = config.get_config('date_bar_develop_hist_file_path')[hk_date_bar_version]
    hk_date_bar_by_compose = pd.read_pickle(hk_date_bar_file_path)
    
    # 评估compose和em的差异
    err_percent_compose_em = evaluate_date_bar_by_rela_err(hk_date_bar_by_compose,hk_date_bar_em)
    
    # 评估compose和sina的差异
    err_percent_compose_sina = evaluate_date_bar_by_rela_err(hk_date_bar_by_compose,hk_date_bar_sina)
    
if __name__ == "__main__":
      # 用于参考比较评估的数据
    hk_date_bar_em = pd.read_pickle(hk_date_bar_em_file_path)
    hk_date_bar_sina = pd.read_pickle(hk_date_bar_sina_file_path)
    # 由分钟线合成的日线数据
    hk_date_bar_version = 'v10'
    hk_date_bar_file_path = config.get_config('date_bar_develop_hist_file_path')[hk_date_bar_version]
    hk_date_bar_by_compose = pd.read_pickle(hk_date_bar_file_path)
    
    # 评估compose和em的差异
    err_percent_compose_em = evaluate_date_bar_by_rela_err(hk_date_bar_by_compose,hk_date_bar_em)
        
    # 评估compose和sina的差异
    err_percent_compose_sina = evaluate_date_bar_by_rela_err(hk_date_bar_by_compose,hk_date_bar_sina)
    
    
    # 绘制日线图，比较差异
    stk = 'hk00001'
    start_date = '2016-01-01'
    end_date = '2022-12-31'
    evaluate_df_ylabel = 'compose_price'
    # 绘制compose和em的日线图，比较差异
    em_df_ylabel = 'em_price'
    evaluate_date_bar_by_plot_date_bar(hk_date_bar_by_compose,hk_date_bar_em,stk,start_date,end_date,evaluate_df_ylabel,em_df_ylabel)
    # 绘制compose和sina的日线图，比较差异
    sina_df_ylabel = 'sina_price'
    evaluate_date_bar_by_plot_date_bar(hk_date_bar_by_compose,hk_date_bar_sina,stk,start_date,end_date,evaluate_df_ylabel,sina_df_ylabel)
    
    
    
