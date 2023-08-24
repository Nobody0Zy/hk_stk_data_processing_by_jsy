trade_session_change_date_dict = {
    '2001-01-01': (('10:01', '12:30'), ('14:31', '16:00')),
    '2011-03-07': (('09:31', '12:00'), ('13:31', '16:00')),
    '2012-03-05': (('09:31', '12:00'), ('13:01', '16:00')),
}


# 历史开发路径
# 分钟线历史开发路径==========================================================
min_bar_develop_hist_folder_path = {
    # 原始数据路径--以股票代码保存的分钟线
    'v01': 'D:\\QUANT_GAME\\python_game\\pythonProject\\DATA\\local_develop_data\\stock\\HK_stock_data\\jsy_develop_hist_data\\min_bar\\v01_raw_data_stk_csv',
    # 原始数据转换为pkl格式后的路径
    'v02': 'D:\\QUANT_GAME\\python_game\\pythonProject\\DATA\\local_develop_data\\stock\\HK_stock_data\\jsy_develop_hist_data\\min_bar\\v02_raw_data_date_pkl',   
    # 将pkl文件时间轴标准化后（包括合并盘前盘后竞价数据）的路径
    'v10': 'D:\\QUANT_GAME\\python_game\\pythonProject\\DATA\\local_develop_data\\stock\\HK_stock_data\\jsy_develop_hist_data\\min_bar\\v10_raw_data_date_pkl_format',
}


# 日线历史开发路径==========================================================
date_bar_develop_hist_file_path = {
    # 标准化分钟线数据合成日线数据后的路径
    'v10': 'D:\\QUANT_GAME\\python_game\\pythonProject\\DATA\\local_develop_data\\stock\\HK_stock_data\\jsy_develop_hist_data\\date_bar\\v10_date_bar.pkl',
}