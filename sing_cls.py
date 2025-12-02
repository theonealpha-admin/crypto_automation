from data_class import StockData
from binance.client import Client
from abc import ABC, abstractmethod
from config import Config
import numpy as np
import polars as pl
import pandas as pd
import os
from binance.enums import *

class sgl_temp(ABC):
    def __init__(self, lookback, std):
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
    def __init__(self, lookback=20, std=2):
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

class execute(Signals, Config):
    def __init__(self, pair, lookback=20, std=2):
        self.pair = pair
        super().__init__(lookback, std)
        self.sgl = Signals(lookback, std)
        self.df = self.sgl.get_data(self.pair)
        self.mean, self.upper, self.lower = self.sgl.calculate_signals(self.pair, self.df)
        self.sngl, self.df, self.mean, self.upper, self.lower = self.sgl.find_sing(
            self.mean, self.upper, self.lower, self.df
        )
        
        self.sym1, self.sym2 = self.pair.split('_')
        
        api_key = 'xD7T3AtRLleOfXgCtWDVbZ7LphRCaV17FtThGsWeWXJ0P30wER4MeVfCJdcXYGP1'
        api_secret = 'VWBbrW4C5oxRRqLlpo3tv2pyhX0HA8t0cuy8vN24kvr7GAYNKbRkczej1d92Zunr'
        
        self.client = Client(api_key, api_secret)
        self.client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'

    def get_qty(self, symbol, fund=4999):
        price = float(self.client.futures_symbol_ticker(symbol=symbol)['price'])
        qty = fund / price
        
        if 'BTC' in symbol:
            return round(qty, 1)
        else:
            return round(qty, 1)
        
    def trade(self):
        print(f"Signal: {self.sngl}")
        
        if self.sngl == "BUY":
            qty1 = self.get_qty(self.sym1)
            qty2 = self.get_qty(self.sym2)
            
            self.client.futures_create_order(symbol=self.sym1, side=SIDE_BUY, type=ORDER_TYPE_MARKET, quantity=qty1)
            self.client.futures_create_order(symbol=self.sym2, side=SIDE_SELL, type=ORDER_TYPE_MARKET, quantity=qty2)
            print(f"BUY {self.sym1}, SELL {self.sym2}")
            
        elif self.sngl == "SELL":
            qty1 = self.get_qty(self.sym1)
            qty2 = self.get_qty(self.sym2)
            
            self.client.futures_create_order(symbol=self.sym1, side=SIDE_SELL, type=ORDER_TYPE_MARKET, quantity=qty1)
            self.client.futures_create_order(symbol=self.sym2, side=SIDE_BUY, type=ORDER_TYPE_MARKET, quantity=qty2)
            print(f"SELL {self.sym1}, BUY {self.sym2}")

if __name__ == "__main__":
    td = execute('BTCUSDT_ETHUSDT')
    td.trade()

