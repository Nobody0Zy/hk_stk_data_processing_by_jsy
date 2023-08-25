import os
import sys
from multiprocessing import Pool
from typing import Dict, List, Set, Tuple, Union

import numpy as np
import pandas as pd

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject")
from my_tools_packages import MyDecorator as MD

running_time = MD.running_time

import logging

logging.basicConfig(
    filename='error_min_bar_stk_df.log',
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    # filemode='w+'
    )

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\common")
import config
import utilities as utl


def _gen_daily_price_data_from_min_bar_stk_df(min_bar_stk_df):
    """ 
    通过分钟线数据生成日线数据
    """
    
    min_bar_stk_df_open_dropna = min_bar_stk_df['open'].dropna()
    if min_bar_stk_df_open_dropna.empty:
        logging.error('min_bar_stk_df的开盘价全为nan值，数据可能有误，请检查, min_bar_stk_df:\n%s',min_bar_stk_df)
        open_price = np.nan
    else:
        open_price = min_bar_stk_df_open_dropna.iloc[0]
    # 生成日线数据
    date_price_volume_data_dict = {
        'open': open_price,
        'high': min_bar_stk_df['high'].max(),
        'low': min_bar_stk_df['low'].min(),
        'close': min_bar_stk_df['close'].iloc[-1],
        'volume': min_bar_stk_df['volume'].sum(),
        'amount': min_bar_stk_df['amount'].sum(),
    }
    date = min_bar_stk_df.index.get_level_values('date_time')[0]//10000
    return pd.DataFrame(date_price_volume_data_dict,index=[date])


def compose_date_bar_by_format_min_bar(min_bar_stk_df):
    """
    通过分钟线数据合成日线数据
    """
    date_bar_df_by_min_bar = min_bar_stk_df.groupby('stk',group_keys=True).apply(
        _gen_daily_price_data_from_min_bar_stk_df)
    return date_bar_df_by_min_bar


def process_file(file_path):
    """ 
    处理单个文件
    """
    min_bar_df = pd.read_pickle(file_path)
    date_bar_df = compose_date_bar_by_format_min_bar(min_bar_df)
    return date_bar_df


def get_file_paths(folder_path:str)->List[str]:
    """ 
    获取文件夹下所有pkl文件的路径
    """
    
    file_list = os.listdir(folder_path)
    file_list = [file for file in file_list if file.endswith('.pkl')]
    file_path_list = [os.path.join(folder_path,file) for file in file_list]
    return file_path_list


def save_date_bar_df(date_bar_df_total,save_folder_path):
    date_bar_df_total.sort_index(inplace=True)
    date_bar_df_total.index.names= ['stk','date']
    date_bar_df_total.to_pickle(save_folder_path)
    logging.info(f'save date_bar_df_total to {save_folder_path} successfully!')   

@running_time
def main():
    # 开发历史分钟线数据（存储于机械硬盘）
    # MIN_BAR_FOLDER_PATH = config.min_bar_develop_hist_folder_path['v10']
    # 开发临时分钟线数据（存储于固态硬盘，提升读写速度）
    MIN_BAR_FOLDER_PATH = config.min_bar_temp_folder_path['v10']
    # 合成日线存储路径，历史开发路径，存储于机械硬盘
    DATE_BAR_FOLDER_PATH = config.date_bar_develop_hist_file_path['v10']
    num_process = 10
    
    need_compose_min_bar_file_path_list = get_file_paths(MIN_BAR_FOLDER_PATH)
    
    with Pool(num_process) as pool:
        date_bar_df_list = pool.map(process_file,need_compose_min_bar_file_path_list)
    
    date_bar_df_total = pd.concat(date_bar_df_list)
    save_date_bar_df(date_bar_df_total,DATE_BAR_FOLDER_PATH)
    logging.info('all done!')
    
if __name__ == '__main__':
    print('The game is on!')
    main()
    