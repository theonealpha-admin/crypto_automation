from data_class import StockData
from abc import ABC, abstractmethod
import numpy as np
import polars as pl
import pandas as pd
import os

class sgl_temp(ABC):
    def __init__(self, lookback=120, std=2):
        self.lookback = lookback
        self.num_std = std
        self.stock = StockData()
    
    @abstractmethod
    def get_data(self, pair):
        pass

    @abstractmethod
    def calculate_signals(self, pair, df):
        pass

    @abstractmethod
    def find_sing(self, mean, upper, lower, df):
        pass

class Signals(sgl_temp):
    def __init__(self, lookback=120, std=2):
        super().__init__(lookback, std)

    def get_data(self, pair):
        df = self.stock.get_spreads(pair)
        return df

    def calculate_signals(self, pair, df):
        series = df['close'].to_pandas()
        mean = series.rolling(window=self.lookback).mean()
        std = series.rolling(window=self.lookback).std()
        upper = mean + (self.num_std * std)
        lower = mean - (self.num_std * std)
        return mean, upper, lower

    def find_sing(self, mean, upper, lower, df):
        spread = df['close'][-1]
        if spread > upper.iloc[-1]:
            return "SELL", df[-1], mean, upper, lower,
        elif spread < lower.iloc[-1]:
            return "BUY", df[-1], mean, upper, lower,
        else:
            return "HOLD", df[-1], mean, upper, lower,

sgl=Signals()
pair='BTCUSDT_ETHUSDT'
df = sgl.get_data(pair)
mean, upper, lower = sgl.calculate_signals(pair, df)
sngl, df, mean, upper, lower = sgl.find_sing(mean, upper, lower, df)
print("sngl", sngl)
print("df", df.tail(1))
print("mean", mean.tail(1))
print("upper", upper.tail(1))
print("lower", lower.tail(1))
