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
    filename='compose_error.log',
    level=logging.ERROR,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    # filemode='w+'
    )

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\common")

import config
import utilities as utl
from ComposeDateBar import ComposeDateBar as ComposeDateBar


def gen_daily_price_data_from_min_bar_stk_df(min_bar_stk_df):
    """ 
    通过分钟线数据生成日线数据
    """
    
    # 生成日线数据
    date_price_volume_data_dict = {
        'open': min_bar_stk_df['open'].iloc[0],
        'high': min_bar_stk_df['high'].max(),
        'low': min_bar_stk_df['low'].min(),
        'close': min_bar_stk_df['close'].iloc[-1],
        'volume': min_bar_stk_df['volume'].sum(),
        'amount': min_bar_stk_df['amount'].sum(),
    }
    date = min_bar_stk_df.index.get_level_values('date_time')[0]//10000
    return pd.DataFrame(date_price_volume_data_dict,index=[date])

@running_time
def main():
    data_version = 'v09'
    compose_method = gen_daily_price_data_from_min_bar_stk_df
    min_bar_folder_path = "F:\\local_tmp_data\\stock\\HK\\v09"
    date_bar_folder_path = ("D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy"
                            "\\detect_and_fix_outliers_data\\nan_values\\compoose_date_bar_not_fillna\\date_bar.pkl")
    
    compose_v11_date_bar = ComposeDateBar(compose_method,min_bar_folder_path,date_bar_folder_path)
    compose_v11_date_bar.multiprocessing_process_file(15)
    
if __name__ == '__main__':
    main()
    