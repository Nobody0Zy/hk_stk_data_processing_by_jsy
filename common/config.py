from constants import *

# Path: common\constants.py

CONFIG = {
    'trade_session_change_date': trade_session_change_date_dict,
    'min_bar_develop_hist_folder_path': min_bar_develop_hist_folder_path,
    'date_bar_develop_hist_file_path': date_bar_develop_hist_file_path,
    'min_bar_temp_folder_path': min_bar_temp_folder_path,
}


def get_config(key):
    return CONFIG[key]