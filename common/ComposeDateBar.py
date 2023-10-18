import _pickle
import logging
import os
import sys
from multiprocessing import Pool
from typing import List

import numpy as np
import pandas as pd


class ComposeDateBar:
    """
    将分钟线合成日线
    """

    def __init__(self, compose_method, input_min_bar_folder_path, output_date_bar_folder_path):
        self.compose_method = compose_method
        self.input_min_bar_folder_path = input_min_bar_folder_path
        self.output_date_bar_folder_path = output_date_bar_folder_path

    def _process_file(self, file_path):
        try:
            min_bar_df = pd.read_pickle(file_path)
        except _pickle.UnpicklingError as e:
            print(f"Error readding{file_path}:{e}")
            raise
        date_bar_df = min_bar_df.groupby('stk', group_keys=True).apply(self.compose_method)
        return date_bar_df

    def _get_file_paths(self, folder_path: str) -> List[str]:
        file_list = os.listdir(folder_path)
        file_list = [file for file in file_list if file.endswith('.pkl')]
        file_path_list = [os.path.join(folder_path, file) for file in file_list]
        return file_path_list

    def multiprocessing_process_file(self, num_process):
        need_compose_file_path_list = self._get_file_paths(self.input_min_bar_folder_path)

        with Pool(num_process) as pool:
            date_bar_df_list = pool.map(self._process_file, need_compose_file_path_list)

        date_bar_df_total = pd.concat(date_bar_df_list)
        date_bar_df_total.index.names = ['stk', 'date']
        date_bar_df_total.sort_index(inplace=True)
        date_bar_df_total.to_pickle(self.output_date_bar_folder_path)
