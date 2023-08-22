from constants import *

# Path: common\constants.py

CONFIG = {
    'trade_session_change_date': trade_session_change_date_dict,
    'min_bar_develop_hist_floder_path': min_bar_develop_hist_floder_path,
    'date_bar_develop_hist_file_path': date_bar_develop_hist_file_path,
}


def get_config(key):
    return CONFIG[key]