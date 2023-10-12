import os
import sys
from typing import List

import numpy as np
import pandas as pd

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject")
from my_tools_packages import MyDecorator as MD

running_time = MD.running_time

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\common")
import config
import utilities as utl
from EvaluateDateBar import EvaluateDateBar as EvalDateBar


class MainEvaluator:
    def __init__(self) -> None:
        self.hk_date_bar_version = 'v10'
        # 获取em和sina的日线数据
        self.hk_date_bar_em_file_path = config.get_config('date_bar_other_source_file_path')['eastmoney_nfq_by_ak']
        self.hk_date_bar_sina_file_path = config.get_config('date_bar_other_source_file_path')['sina_nfq_by_ak']
        self.hk_date_bar_em = pd.read_pickle(self.hk_date_bar_em_file_path)
        self.hk_date_bar_sina = pd.read_pickle(self.hk_date_bar_sina_file_path)
        # 获取由分钟线合成的日线数据
        self.hk_date_bar_file_path = config.get_config('date_bar_develop_hist_file_path')[self.hk_date_bar_version]
        self.hk_date_bar_by_compose = pd.read_pickle(self.hk_date_bar_file_path)
        # 评估错误的阈值
        self.threshold = 0.05
        
        # 保存评估结果的文件夹路径
        self.save_evaluate_res_folder_path = \
            f"D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\compose_and_evaluate_date_bar\\{self.hk_date_bar_version}\\"
        
        self.plot_info = {
            'stk': 'hk03690',
            'start_date': 20110101,
            'end_date': 20231231,
            'save_fig': True,
            'save_png_folder_path': self.save_evaluate_res_folder_path+'\\res\\plot',
        }
    
    def plot_evaluations(self, eval_compose_em, eval_compose_sina):
        eval_compose_em.plot_date_bar(**self.plot_info)
        eval_compose_sina.plot_date_bar(**self.plot_info)
        
    def save_err_percent_to_csv(self, err_percent_df_list: List[pd.DataFrame],cols: List[str], save_file_path: str):
        err_percent_total_df = pd.concat(err_percent_df_list, axis=1)
        err_percent_total_df.columns = cols
        err_percent_total_df.to_csv(save_file_path)
        
    def evaluate(self):
        # 评估compose和em的差异
        eval_compose_em = EvalDateBar(self.hk_date_bar_by_compose, self.hk_date_bar_em, self.hk_date_bar_version, 'em')
        eval_compose_em_err_percent = eval_compose_em.calc_err_percent(self.threshold)
        # 评估compose和sina的差异
        eval_compose_sina = EvalDateBar(self.hk_date_bar_by_compose, self.hk_date_bar_sina, self.hk_date_bar_version, 'sina')
        eval_compose_sina_err_percent = eval_compose_sina.calc_err_percent(self.threshold)
        # 保存评估结果
        err_percent_df_list = [eval_compose_em_err_percent, eval_compose_sina_err_percent]
        res_file_name = self.hk_date_bar_version + '_err_percent.csv'
        save_res_cols = [f'{self.hk_date_bar_version}_vs_em', f'{self.hk_date_bar_version}_vs_sina']
        save_res_file_path = os.path.join(self.save_evaluate_res_folder_path+'\\res', res_file_name)
        self.save_err_percent_to_csv(err_percent_df_list, save_res_cols, save_res_file_path)
        
        # 画图
        self.plot_evaluations(eval_compose_em, eval_compose_sina)
        
def main():
    main_evaluator = MainEvaluator()
    main_evaluator.evaluate()
    
if __name__ == '__main__':
    print('The game is on!')
    main()