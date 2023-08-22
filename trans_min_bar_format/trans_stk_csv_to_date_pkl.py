# -*- coding: utf-8 -*-
import datetime as dt
import os
import sys
from datetime import datetime
from typing import Tuple

import numpy as np
import pandas as pd
# from my_tools_packages import MyDecorator as MD
from pandarallel import pandarallel

pandarallel.initialize()
# running_time = MD.running_time

idx = pd.IndexSlice

"""
本程序是特殊化的，尽管其中有些功能可以一般化写成类，用于维护和拓展；
但是程序的针对性更强，只用于转换原本的以股票名保存的csv文件转换为以日期保存的pickle文件。
"""


def _trans_datetime_format_to_str(date_time_str: str):
    """
    该函数用于统一日期格式，为'%Y-%m-%d %H:%M:%S'，例如'2011-01-04 09:30:00'
    先将字符串转换为datetime格式，再转换为字符串格式，避免有不匹配的日期格式(2011-1-4)
    该函数是在pandas的apply中使用的，所以需要在函数中导入datetime
    ：param date_time_str: 日期字符串
    ：return date_time_str_res: 转换后的日期字符串
    """
    global date_time_datetime
    from datetime import datetime

    # 如果有'-'，则按照'-'分割，否则按照'/'分割，如果都没有，则报错
    # ---
    # 错误的日期格式有：'2004-00-00 00:00:00', '4-00-00 00:00:00'
    if '-' in date_time_str:
        error_date_time_str_list = \
            ['2004-00-00 00:00:00', '4-00-00 00:00:00']
        if date_time_str in error_date_time_str_list:
            date_time_str = '2004-01-01 00:00:00'
        # 把字符串date_time_str转换为datetime格式
        date_time_datetime = datetime.strptime(date_time_str, '%Y-%m-%d %H:%M:%S')
    elif '/' in date_time_str:
        # 把字符串date_time_str转换为datetime格式
        date_time_datetime = datetime.strptime(date_time_str, '%Y/%m/%d %H:%M:%S')
    else:
        print(f'{date_time_str} is not in the right format')
        # 退出程序
        sys.exit()
    
    # === 
    # 日期格式转换
    # 把datetime格式的date_time_datetime转换为字符串格式，例如'2011-01-04 09:30:00'
    date_time_str_res = datetime.strftime(date_time_datetime, '%Y-%m-%d %H:%M:%S')
    return date_time_str_res


# 对单个stk_csv进行处理，按照date进行groupby后存入pkl文件
def trans_column1_stk_concat_df_to_date_pkl(concat_df, output_folder_path):
    """
    该函数用于对columns1文件夹中的数据格式进行标准统一化处理
    并且将文件夹内以stk保存的csv文件转换为以date保存的pkl文件
    :param concat_df: 合并后的dataframe
    :param output_folder_path: 输出路径
    """
    m_df = concat_df.copy()
    del concat_df
    # 将m_df的列名转换为小写
    m_df.columns = [m_column.lower() for m_column in m_df.columns]
    # 调整lie的顺序，使其标准化
    m_df = m_df[['stk', 'date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
    # 对列名date更名为date_time
    m_df = m_df.rename(columns={'date': 'date_time'})
    # 将date_time列的通过_trans_datetime_format函数转换为
    # datetime的字符串格式('%Y-%m-%d %H:%M:%S')
    date_time_list = [_trans_datetime_format_to_str(date_time_str) for date_time_str in m_df['date_time'].values]
    # ---
    # 取出date_time中的日期和时间，存入date和time列
    date_list = [date_time_str.split(' ')[0] for date_time_str in date_time_list]
    time_list = [date_time_str.split(' ')[1] for date_time_str in date_time_list]
    m_df['date_time'] = date_time_list
    try:
        m_df.insert(2, 'date', date_list)
        m_df.insert(3, 'time', time_list)
    except Exception as e:
        print(e)
        print('date has been inserted')
    # ---
    # 将m_df按照date进行groupby，然后将每个group存入pkl文件
    m_df.groupby('date').apply(lambda df: df.to_pickle(os.path.join(output_folder_path,df['date'].values[0] + '.pkl')))
    print(f'{output_folder_path} has been saved')


# columns1和columns2的格式不同，需要分开单独处理
def trans_columns2_stk_concat_df_to_date_pkl(concat_df, output_folder_path):
    """
    该函数用于对columns2文件夹中的数据格式进行标准统一化处理
    并且将文件夹内以stk保存的csv文件转换为以date保存的pkl文件
    ：param concat_df: 合并后的dataframe
    ：param output_folder_path: 输出路径
    """
    m_df = concat_df.copy()
    del concat_df
    # 将m_df的time列的格式转换为'%H:%M:%S'，使其标准化
    m_df.time = m_df.time + ':00'
    # 此处添加date_time列，用于后续合成索引
    try:
        m_df.insert(1, 'date_time', m_df.date + ' ' + m_df.time)
    except Exception as e:
        print(e)
        print('date_time has been inserted')
    # 对m_df的date_time列进行格式转换，转换为'%Y-%m-%d %H:%M:%S'的字符串格式
    m_df['date_time'] = m_df['date_time'].parallel_apply(_trans_datetime_format_to_str)
    # 取出date_time中的日期和时间，存入date和time列
    date_list = [date_time_str.split(' ')[0] for date_time_str in m_df['date_time'].values]
    time_list = [date_time_str.split(' ')[1] for date_time_str in m_df['date_time'].values]
    m_df['date'] = date_list
    m_df['time'] = time_list
    # 将m_df按照date进行groupby，然后将每个group存入pkl文件
    m_df.groupby('date').apply(lambda df: df.to_pickle(os.path.join(output_folder_path,df['date'].values[0] + '.pkl')))
    print(f'{output_folder_path} has been saved')



def _get_hk_stk_list():
     # 根据港股股票代码的规则，构造港股股票代码的集合
    hk_stk_list = [f'hk{i:05d}' for i in range(1, 10000)]
    hk_rmb_stk_list = [f'hk8{i:04d}' for i in range(1, 10000)]
    hk_stk_list = hk_stk_list + hk_rmb_stk_list
    return hk_stk_list


def concat_stk_csv_file_list_to_dataframe(stk_csv_file_path):
    """ 
    该函数是将stk_csv_file_path文件夹中的所有csv文件合并成一个dataframe
    :param stk_csv_file_path: csv文件所在的文件夹路径
    :return: 合并后的dataframe
    """
    # 获取stk_csv_file_path文件夹中的所有csv文件名称
    stk_csv_file_path_list = os.listdir(stk_csv_file_path)
    stk_df_file_list = [] # 用于存储所有的stk_df
    
    # 获取港股股票代码
    hk_stk_list = _get_hk_stk_list()
    
    # 遍历stk_csv_file_path文件夹中的所有csv文件
    for stk_csv_file in stk_csv_file_path_list:
        # 获取stk_csv_file的股票代码
        stk = stk_csv_file.split('\\')[-1].split('.')[0].lower()
        # 如果stk是港股股票代码
        if stk in hk_stk_list:
            # 打印文件夹内csv文件的处理进度
            print('%.2f%%' %
                  (stk_csv_file_path_list.index(stk_csv_file) / len(stk_csv_file_path_list) * 100))
            # 读取columns1和columns2格式的csv文件
            if 'columns1' in stk_csv_file_path:
                stk_df = pd.read_csv(os.path.join(stk_csv_file_path, stk_csv_file))
            if 'columns2' in stk_csv_file_path:
                stk_df = pd.read_csv(
                    os.path.join(stk_csv_file_path, stk_csv_file),
                    header=None, names=['date', 'time', 'open', 'high', 'low', 'close', 'volume', 'amount'])
            # ------------------------
            # 尝试将stk代码插入到stk_df的第一列,若已经存在则跳过
            try:
                stk_name = stk_csv_file.split('\\')[-1].split('.')[0].lower()
                print(stk_name)
                stk_df.insert(0, 'stk', stk_name)
            except Exception as e:
                print(e)
                print(f'{stk_csv_file}已经存在')
        else:
            print(f'{stk_csv_file}不需要读取')
            continue
        # 将stk_df存入stk_df_file_list
        stk_df_file_list.append(stk_df)
    # 将stk_df_file_list中的所有stk_df进行concat
    stk_df_concat_res_df = pd.concat(stk_df_file_list)
    print('stk_df_concat_res_df has been concatenated')
    del stk_df_file_list
    return stk_df_concat_res_df

def split_concat_res_df_by_date(concat_res_df, split_num):
    """
    将concat合并后过大的dataframe按照date进行分割，分割成split_num份
    :param concat_res_df: 对文件夹内分钟线合并后的dataframe
    :param split_num: 分割的份数
    :return: 分割后的dataframe列表
    """
    # 取出concat_res_df中的date列，去重后排序，得到date列表
    concat_res_df_date_list = list(concat_res_df.date.unique())
    concat_res_df_date_list.sort()
    # 把concat_res_df_date_list均分成splt_num份
    concat_res_df_date_list_split = np.array_split(concat_res_df_date_list, split_num)
    concat_res_df_split_by_date_list = []
    # 根据concat_res_df_date_list_split中的每个list，
    # 取出concat_res_df中对应的date的行，存入concat_res_df_split_by_date_list
    for split_list in concat_res_df_date_list_split:
        concat_res_df_split_by_date = concat_res_df[concat_res_df.date.isin(split_list)]
        concat_res_df_split_by_date_list.append(concat_res_df_split_by_date)
        del concat_res_df_split_by_date
    del concat_res_df
    # 返回存着分割后的dataframe的列表
    return concat_res_df_split_by_date_list



if __name__ == '__main__':
    jsy_raw_data_stk_csv_folder_path = \
        'D:\\Anaconda3\\pythonProject\\DATA\\HK_stock_data\\jsy_data_progress_tasks\\jsy_raw_data_stk_csv'
    # 遍历jsy_raw_data_stk_csv文件夹中的所有文件夹
    for root, dirs, files in os.walk(jsy_raw_data_stk_csv_folder_path, topdown=True):
        new_temp_root = root.replace('jsy_raw_data_stk_csv', 'jsy_raw_data_v0_date_pkl')
        # 如果临时路径不存在
        if not os.path.exists(new_temp_root):
            os.makedirs(new_temp_root)
        if not dirs:
            print(new_temp_root + '已经存在')
            # 将root文件夹中的所有csv文件合并成一个dataframe
            concat_res_df = concat_stk_csv_file_list_to_dataframe(root)
            if 'columns1' in root:
                # 转换合并后的dataframe的格式，按照date进行groupby后存入pkl文件
                trans_column1_stk_concat_df_to_date_pkl(concat_res_df, new_temp_root)
            elif 'columns2' in root:
                if len(concat_res_df) > 10 ** 8:
                    # 如果concat_res_df的行数大于10亿，则按照日期分割成5份
                    concat_res_df_split_by_date_list = split_concat_res_df_by_date(concat_res_df, 5)
                    for concat_split_by_date_df in concat_res_df_split_by_date_list:
                        # 转换合并后的dataframe的格式，按照date进行groupby后存入pkl文件
                        trans_columns2_stk_concat_df_to_date_pkl(concat_split_by_date_df, new_temp_root)
                else:
                    # 转换合并后的dataframe的格式，按照date进行groupby后存入pkl文件
                    trans_columns2_stk_concat_df_to_date_pkl(concat_res_df, new_temp_root)
                    
# ========================================================================================
# 找出文件夹内文件大小异常的文件
# 获取一个文件夹内所有文件的大小，单位为MB
