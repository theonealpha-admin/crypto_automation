from data_class import StockData
from abc import ABC, abstractmethod
from statsmodels.regression.rolling import RollingOLS
from datetime import datetime, timedelta
import time
from config import Config
import numpy as np
import pandas as pd
import os
from db import DB

class Spreads_temp(ABC):
    def __init__(self):
        super().__init__()
        self.stock = StockData()
        self.db=DB()
    
    @abstractmethod
    def get_spd_data(self, pair):
        pass

    @abstractmethod
    def calculate_spread(self, pair):
        pass

class Spreads(Spreads_temp, Config):
    def __init__(self):
        super().__init__()

    def get_spd_data(self, pair):
        s1, s2 = pair.split('_')
        df1 = self.stock.get(s1)
        df2 = self.stock.get(s2)
        return s1, s2, df1, df2

    def prepare_data_for_spread(self, s1, s2, df1, df2, pair):
        last_date = self.db.spreads_last_row(pair)
        if last_date:
            idx1 = max(0, np.searchsorted(df1['date'], last_date + 1) - self.window * 2)
            idx2 = max(0, np.searchsorted(df2['date'], last_date + 1) - self.window * 2)
            df1, df2 = df1[idx1:], df2[idx2:]
        common_dates = np.intersect1d(df1['date'], df2['date'])
        if len(common_dates) < self.window:
            print(f"Not enough common dates for {pair}: {len(common_dates)}")
            return None
        i1 = np.searchsorted(df1['date'], common_dates)
        i2 = np.searchsorted(df2['date'], common_dates)
        x = np.log(df1['close'][i1])
        y = np.log(df2['close'][i2])
        x_open = np.log(df1['open'][i1])
        y_open = np.log(df2['open'][i2])
        return common_dates, x, y, x_open, y_open

    def calculate_spread(self, s1, s2, df1, df2, pair, live=False):
        prep = self.prepare_data_for_spread(s1, s2, df1, df2, pair)
        if not prep:
            return np.array([], dtype=[('date', 'i8'), ('open', 'f4'), ('close', 'f4'), ('hedge_ratio', 'f4')])
        common_dates, x, y, x_open, y_open = prep
        hedge_ratios = RollingOLS(y, x, window=self.window).fit().params.squeeze()
        
        spd_close = y - (hedge_ratios * x)
        spd_open = y_open - (hedge_ratios * x_open)
        
        sp_arr = np.array(
            list(zip(common_dates, spd_open, spd_close, hedge_ratios)),
            dtype=[('date', 'i8'), ('open', 'f4'), ('close', 'f4'), ('hedge_ratio', 'f4')]
        )
        sp_arr = sp_arr[~np.isnan(sp_arr['open']) & ~np.isnan(sp_arr['close'])]
        # print(f"sp_arr {len(sp_arr)}, {s1}_{s2}")
        self.db.save_spreads(sp_arr, f"{s1}_{s2}", live)
        return sp_arr
