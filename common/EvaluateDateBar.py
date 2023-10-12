
import datetime
import math
import os
import sys

import mplfinance as mpf
import numpy as np
import pandas as pd
from my_tools_packages import MyDecorator as MD

running_time = MD.running_time
idx = pd.IndexSlice

class EvaluateDateBar:
    def __init__(self,evaluate_date_bar:pd.DataFrame,other_source_date_bar:pd.DataFrame,evaluated_date_bar_version,
                 other_source_dat_bar_version)-> None:
        self.evaluate_date_bar = evaluate_date_bar
        self.other_source_date_bar = other_source_date_bar       
        self.evaluated_date_bar_version = evaluated_date_bar_version
        self.other_source_date_bar_version = other_source_dat_bar_version
        
     
    def _calc_relative_err(self):
        df1 = self.evaluate_date_bar
        df2 = self.other_source_date_bar
        # 获取相同的index 
        idx_inter = df1.index.intersection(df2.index).to_list()
        idx_inter.sort()
        # 获取相同的columns
        col_inter = df1.columns.intersection(df2.columns)
        col_list = ['relative_err_' + col for col in col_inter]
        # 生成空的DataFrame
        res_df = pd.DataFrame(index=idx_inter, columns=col_list)
        for col in col_inter:
            res_df['relative_err_' + col] = np.abs(df1.loc[idx_inter, col] - df2.loc[idx_inter, col]) / df2.loc[idx_inter, col]
        return res_df
    
    def calc_err_percent(self,threshold:float=0.05) -> int:
        res_df = self._calc_relative_err()
        res_err_df= res_df[res_df>threshold].dropna(how='all')
        return res_err_df.count()/res_df.count()
    
    @staticmethod
    def __trans_date_bar_format(stk_df_list):
        stk_df_format_list = []
        def int2date(idate):
            return datetime.datetime(math.floor(idate / 10000), math.floor(idate % 10000 / 100),
                                     math.floor(idate % 100))
        for stk_df in stk_df_list:
            stk_df.index = pd.DatetimeIndex([int2date(x) for x in stk_df.index.get_level_values('date')])
            stk_df.columns = [x.title() for x in stk_df.columns]
            stk_df_format_list.append(stk_df)
        return stk_df_format_list
    
    
    def _plot_date_bar(self,stk_df_list,title,df1_ylabel,df2_ylabel,save_fig=False,save_png_file_path=None):
        stk_df_format_list = self.__trans_date_bar_format(stk_df_list)
        # 添加幅图,即被比较的数据的k线图
        add_plot = [
            mpf.make_addplot(stk_df_format_list[1], type='candle', panel=1, ylabel=df2_ylabel)] 
        # 设置颜色
        my_color = mpf.make_marketcolors(up='red', down='green', edge='i', volume='in', inherit=False)
        # 设置风格
        my_style = mpf.make_mpf_style(base_mpf_style='blueskies', gridaxis='both', gridstyle='-.',
                                      gridcolor='c',marketcolors=my_color, y_on_right=False, 
                                        rc={'font.family': 'KaiTi','axes.unicode_minus': False})

        if not save_fig:
            # 绘制
            mpf.plot(stk_df_format_list[0], type='candle', title={'title': title, 'y': 0.98},
                        ylabel=df1_ylabel,style=my_style, volume=False, tight_layout=True,
                        addplot=add_plot, main_panel=0,panel_ratios = (1,1),
                        figratio=(1.618, 1), figscale=1.3, datetime_format='%Y-%m-%d',
                        )
        else:
            if save_png_file_path is None:
                raise ValueError('save_png_file_path is None!')
            mpf.plot(stk_df_format_list[0],type='candle',title={'title': title, 'y': 0.98},
                        ylabel=df1_ylabel,style=my_style, volume=False, tight_layout=True,
                        addplot=add_plot, main_panel=0,panel_ratios=(1,1),
                        figratio=(1.618, 1), figscale=1.3, datetime_format='%Y-%m-%d',
                        savefig=save_png_file_path
                        )


    # 绘制stk的k线图 
    def plot_date_bar(self,stk,start_date,end_date,save_fig=False,save_png_folder_path=None):
        evaluate_date_bar_stk_df = self.evaluate_date_bar.loc[idx[stk,start_date:end_date],:]
        other_source_date_bar_stk_df = self.other_source_date_bar.loc[idx[stk,start_date:end_date],:]
        stk_df_list = [evaluate_date_bar_stk_df,other_source_date_bar_stk_df]
        
        title = stk + '_' + str(start_date) + '_' + str(end_date)+'_vs_'+self.other_source_date_bar_version
        file_name = title + '.png'
        evaluated_df_ylabel = self.evaluated_date_bar_version + '_price'
        other_source_df_ylabel = self.other_source_date_bar_version + '_price'
        save_png_file_path = os.path.join(save_png_folder_path,self.evaluated_date_bar_version,file_name)
        
        if not save_fig:
            self._plot_date_bar(stk_df_list,title,evaluated_df_ylabel,other_source_df_ylabel)
        else:
            self._plot_date_bar(stk_df_list,title,evaluated_df_ylabel,other_source_df_ylabel,save_fig,save_png_file_path)
        