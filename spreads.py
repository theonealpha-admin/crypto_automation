from data_class import StockData
from abc import ABC, abstractmethod
from statsmodels.regression.rolling import RollingOLS
import numpy as np
import polars as pl
import os

class Spreads_temp(ABC):
    def __init__(self, window=120):
        self.window = window
        self.stock = StockData()
    
    @abstractmethod
    def get_spd_data(self, pair):
        pass

    @abstractmethod
    def calculate_spread(self, pair):
        pass

class Spreads(Spreads_temp):
    def __init__(self,window=120):
        super().__init__(window)

    def get_spd_data(self, pair):
        s1, s2 = pair.split('_')
        ldt = self.stock.last_spd_data(pair)
        if ldt:
            df1 = self.stock.get(s1)
            start_date = df1.filter(pl.col("date") <= ldt).tail(self.window)[0, "date"]
            df1 = df1.filter(pl.col("date") >= start_date)
            df2 = self.stock.get(s2)
            df2 = df2.filter(pl.col("date") >= start_date)
        else:
            df1 = self.stock.get(s1)
            df2 = self.stock.get(s2)
        return s1, s2, df1, df2


    def calculate_spread(self, s1, s2, df1, df2):
        df = df1.join(df2, on="date", how="inner", suffix="_2")
        dates = df["date"]
        x = np.log(df["close"].to_numpy())
        y = np.log(df["close_2"].to_numpy())
        x_open = np.log(df["open"].to_numpy())
        y_open = np.log(df["open_2"].to_numpy())
        model = RollingOLS(y, x, window=self.window)
        results = model.fit()
        hedge_ratios = results.params.squeeze()
        spd_close = y - (hedge_ratios * x)
        spd_open = y_open - (hedge_ratios * x_open)
        sp_df = pl.DataFrame({
            'date': dates,
            'close': spd_close,
            'open': spd_open
        })

        pair = f"{s1}_{s2}"
        self.stock.save_spreads(sp_df, pair)
        return sp_df

spd=Spreads()
pair='BTCUSDT_ETHUSDT'
s1, s2, df1, df2 = spd.get_spd_data(pair)
sp_df = spd.calculate_spread(s1, s2, df1, df2)
# print("sp_df", sp_df)
# print(len(sp_df))