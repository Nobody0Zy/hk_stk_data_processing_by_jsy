# -*- coding: utf-8 -*-
import datetime as dt
import os
import sys

import numpy as np
import pandas as pd

sys.path.append(r"D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\common")
from multiprocessing import Pool

import config
import utilities as utl

idx = pd.IndexSlice


class CalcFqFactor:
    def __init__(self, pre_date_close, qx_df):
        self.pre_date_close_series = pre_date_close
        self.qx_df = qx_df
        self.intersection_idx_of_input = self.pre_date_close_series.index.intersection(self.qx_df.index)

    def get_prepared_qx_df(self):
        df = self.qx_df.copy()
        df.columns = ['sg', 'pg', 'pg_price', 'hl']
        prepared_qx_df = df.loc[self.intersection_idx_of_input]
        return prepared_qx_df

    def _calc_cq_close_price(self, prepared_qx_df):
        cq_close_price = \
            (self.pre_date_close_series.loc[
                 self.intersection_idx_of_input] - prepared_qx_df.hl + prepared_qx_df.pg * prepared_qx_df.pg_price) \
            / (1 + prepared_qx_df.pg + prepared_qx_df.sg)
        return cq_close_price

    def calc_fq_factor(self, prepared_qx_df, fq_method):
        global cq_zdf
        pre_date_cq_close = self._calc_cq_close_price(prepared_qx_df)
        if fq_method == 'hfq':
            cq_zdf = self.pre_date_close_series.loc[self.intersection_idx_of_input] / pre_date_cq_close
        elif fq_method == 'qfq':
            cq_zdf = pre_date_cq_close / self.pre_date_close_series.loc[self.intersection_idx_of_input]
        # 这里是以所有股票数据的第一根开始的，及20110103，如果存在后面重新上市的股票，只需更改此处的累乘即可
        cq_zdf.loc[idx[:, 20110103]] = 1
        fq_factor = cq_zdf.groupby('stk', group_keys=False).apply(lambda s: s.cumprod())
        # if stk == 'hk00539' or 'hk00731':
        #     print('check')
        return fq_factor

    def get_date_fq_factor(self):
        date_fq_factor_series = pd.Series(index=self.pre_date_close_series.index, dtype=np.float64)
        prepared_qx_df = self.get_prepared_qx_df()
        fq_factor = self.calc_fq_factor(prepared_qx_df, 'hfq')
        date_fq_factor_series.loc[idx[:, 20110103]] = 1
        date_fq_factor_series.loc[self.intersection_idx_of_input] = fq_factor
        date_fq_factor_series.fillna(method='ffill', inplace=True)
        return date_fq_factor_series


class GetMinBarAppendCols:

    def __init__(self, min_bar_df, pre_date_close, date_fq_factor):
        self.min_bar_df = min_bar_df
        self.date = min_bar_df.index.get_level_values('date_time')[0] // 10000
        self.pre_date_close = pre_date_close
        self.date_fq_factor = date_fq_factor
        self.min_bar_pre_close = None
        self.min_bar_avg_price = None
        self.min_bar_fq_factor = None

    def __get_pre_close_form_stk_min_bar_df(self, stk_close_series):
        # 如果第一根日线为空，则默认没有交易，用前日收盘价填充
        stk_pre_close_series = stk_close_series.shift(1)
        stk_pre_close_series = stk_pre_close_series.fillna(method='ffill')
        stk = stk_close_series.index[0][0]
        stk_pre_close_series.fillna(self.pre_date_close.loc[idx[stk, self.date]], inplace=True)
        return stk_pre_close_series

    def _get_pre_close_of_min_bar(self):
        self.min_bar_pre_close = self.min_bar_df[['close']].groupby('stk', group_keys=False).apply(
            self.__get_pre_close_form_stk_min_bar_df)

    def __calc_avg_price(self, stk_df):
        return stk_df[['amount']].cumsum() / stk_df[['volume']].cumsum().values

    def _get_avg_price_of_min_bar(self):
        self.min_bar_avg_price = self.min_bar_df.groupby('stk', group_keys=False).apply(
            self.__calc_avg_price
        )

    # def get_
    def __get_stk_date_fq_factor(self, stk_df):
        stk = stk_df.index[0][0]
        # date_fq_factor = self.date_fq_factor.loc[idx[stk,self.date]]
        stk_df.loc[:, 'fq_factor'] = self.date_fq_factor.loc[idx[stk, self.date]]
        return stk_df[['fq_factor']]

    def _get_fq_factor_of_min_bar(self):
        self.min_bar_fq_factor = self.min_bar_df.groupby('stk', group_keys=False).apply(self.__get_stk_date_fq_factor)

    def get_min_bar_append_cols(self):
        self._get_pre_close_of_min_bar()
        self._get_avg_price_of_min_bar()
        self._get_fq_factor_of_min_bar()
        # min_bar_df = self.min_bar_df
        # min_bar_pre_close = self.min_bar_pre_close
        # min_bar_avg_price = self.min_bar_avg_price
        # min_bar_fq_factor = self.min_bar_fq_factor
        append_cols_df = pd.concat([self.min_bar_pre_close, self.min_bar_avg_price, self.min_bar_fq_factor], axis=1)
        append_cols_df.columns = ['pre_close', 'avg_price', 'hfq_factor']
        return append_cols_df


class GetAppendCols:
    def __init__(self, data_version):
        self.data_version = data_version
        self.date_bar_file_path = config.get_config('date_bar_develop_hist_file_path')[self.data_version]
        self.date_bar = pd.read_pickle(self.date_bar_file_path)
        self.min_bar_folder_path = config.get_config('min_bar_develop_hist_folder_path')[self.data_version]
        # self.min_bar_folder_path = "F:\\local_tmp_data\\stock\\HK\\v20"
        self.min_bar_file_list = os.listdir(self.min_bar_folder_path)
        self.qx_df = pd.read_pickle(
            'D:\\QUANT_GAME\\python_game\\pythonProject\\hk_stk_data_processing_codes_by_jsy\\add_append_cols\\HKqx.pkl')
        self.pre_date_close = None
        self.date_hfq_factor = None
        self.append_cols_res_output_path = config.get_config('min_bar_append_cols_folder_path')[self.data_version]
            

    def get_pre_date_close_form_date_bar(self):
        pre_date_price = self.date_bar['close'].groupby('stk', group_keys=False).apply(lambda stk_df: stk_df.shift(1))
        return pre_date_price

    def get_date_hfq_factor(self, pre_date_close):
        calc_hfq_factor = CalcFqFactor(pre_date_close, self.qx_df)
        date_hfq_factor = calc_hfq_factor.get_date_fq_factor()
        return date_hfq_factor

    def get_is_liar(self):
        pass

    def get_append_cols_from_date_bar(self):
        self.pre_date_close = self.get_pre_date_close_form_date_bar()
        self.date_hfq_factor = self.get_date_hfq_factor(self.pre_date_close)

    def get_append_cols_from_single_min_bar_file(self, file_name):
        print(file_name)
        input_file_path = os.path.join(self.min_bar_folder_path, file_name)
        min_bar_df = pd.read_pickle(input_file_path)
        get_min_bar_pre_close = GetMinBarAppendCols(min_bar_df, self.pre_date_close, self.date_hfq_factor)
        min_bar_append_cols_df = get_min_bar_pre_close.get_min_bar_append_cols()
        output_save_path = os.path.join(self.append_cols_res_output_path,file_name)
        # output_save_path = os.path.join('F:\\local_tmp_data\\stock\\HK\\v20_apd_cols\\min_bar', file_name)
        min_bar_append_cols_df.to_pickle(output_save_path)

    def get_append_cols_from_min_bar_folder_by_multiprocessing(self, num_process):
        self.get_append_cols_from_date_bar()
        # file_name = 'min20200521.pkl'
        # self.get_append_cols_from_single_min_bar_file(file_name)
        # for file_name in self.min_bar_file_list:
        #     self.get_append_cols_from_single_min_bar_file(file_name)
        with Pool(num_process) as pool:
            res = pool.map(self.get_append_cols_from_single_min_bar_file, self.min_bar_file_list)


if __name__ == '__main__':
    print('New Game')
    get_append_cols = GetAppendCols('v20')
    # pre_date_price, date_hfq_factor = get_append_cols.pre_date_close, get_append_cols.date_hfq_factor
    get_append_cols.get_append_cols_from_min_bar_folder_by_multiprocessing(16)
