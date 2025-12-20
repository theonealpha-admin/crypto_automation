from data_class import StockData
from binance.client import Client
from binance.enums import *
from abc import ABC, abstractmethod
import matplotlib.pyplot as plt
from config import Config
from config import RedisConnection
from datetime import datetime, timedelta
import time
import json
import csv
import numpy as np
import polars as pl
import pandas as pd
import os
from logger import setup_logger, log_success, log_error

class sgl_temp(ABC):
    def __init__(self):
        super().__init__()
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

class Signals(sgl_temp, Config):
    def __init__(self):
        super().__init__()

    def get_data(self, pair):
        df = self.stock.get_spreads(pair)
        return df

    def plot_chart(self, pair, series, mean, upper, lower):
        plt.figure(figsize=(12, 6))
        plt.plot(series.tail(5000).values, label=f'Close ({pair})', linewidth=2)
        plt.plot(mean.tail(5000).values, label='Mean', linestyle='--')
        plt.plot(upper.tail(5000).values, label='Upper', linestyle=':')
        plt.plot(lower.tail(5000).values, label='Lower', linestyle=':')
        plt.plot(label=pair)
        plt.legend()
        plt.grid(alpha=0.3)
        plt.show()

    def calculate_signals(self, pair, df):
        series = df['close']
        mean = series.rolling(window=self.lookback).mean()
        std = series.rolling(window=self.lookback).std()
        upper = mean + (self.std * std)
        lower = mean - (self.std * std)
        # self.plot_chart(pair, series, mean, upper, lower)
        return mean, upper, lower

    def find_sing(self, mean, upper, lower, df):
        spread = df['close'].iloc[-1]
        if spread > upper.iloc[-1]:
            return "SELL", spread, mean, upper, lower,
        elif spread < lower.iloc[-1]:
            return "BUY", spread, mean, upper, lower,
        else:
            return "HOLD", spread, mean, upper, lower,

class execute(Signals, Config):
    def __init__(self, pair):
        self.pair = pair
        self.logger = setup_logger('trade_logger')
        super().__init__()
        self.rconn = RedisConnection.get_instance()
        self.sgl = Signals()
        self.df = self.sgl.get_data(self.pair)
        self.mean, self.upper, self.lower = self.sgl.calculate_signals(self.pair, self.df)
        self.sngl, self.spread, self.mean, self.upper, self.lower = self.sgl.find_sing(self.mean, self.upper, self.lower, self.df)  
        self.sym1, self.sym2 = self.pair.split('_')
        api_key = 'xD7T3AtRLleOfXgCtWDVbZ7LphRCaV17FtThGsWeWXJ0P30wER4MeVfCJdcXYGP1'
        api_secret = 'VWBbrW4C5oxRRqLlpo3tv2pyhX0HA8t0cuy8vN24kvr7GAYNKbRkczej1d92Zunr'
        self.client = Client(api_key, api_secret)
        self.client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'

    def get_qty(self, symbol):
        price = float(self.client.futures_symbol_ticker(symbol=symbol)['price'])
        qty = self.fund / price
        info = self.client.futures_exchange_info()
        for s in info['symbols']:
            if s['symbol'] == symbol:
                return round(qty, s['quantityPrecision'])
        return round(qty, 3)
        
    def save_trade_to_redis(self, trade_data):
        key = f"trade:{self.pair}"
        self.rconn.set(key, json.dumps(trade_data))

    def log_trade_to_csv(self, qty1, qty2, status="OPEN"):
        os.makedirs('log', exist_ok=True)
        if not os.path.exists('log/trade_history.csv'):
            with open('log/trade_history.csv', 'w', newline='') as f:
                csv.writer(f).writerow(['Timestamp', 'Pair', 'Signal', 'Spread', 'Mean', 'Upper', 'Lower', 'Qty1', 'Qty2', 'Exit_Time', 'Exit_Spread', 'Status'])
        with open('log/trade_history.csv', 'a', newline='') as f:
                csv.writer(f).writerow([datetime.now(), self.pair, self.sngl, self.spread,self.mean.iloc[-1], self.upper.iloc[-1], self.lower.iloc[-1], qty1, qty2, None, None, status])

    def trade(self):
        print(f"Signal: {self.sngl} for pair: {self.pair} | Last Candle: {pd.to_datetime(self.df['date'].iloc[-1], unit='ms').tz_localize('UTC').tz_convert('Asia/Kolkata')}")
        
        if self.sngl == "BUY":
            existing_trade = self.rconn.get(f"trade:{self.pair}")
            if existing_trade:
                return
            qty1 = self.get_qty(self.sym1)
            qty2 = self.get_qty(self.sym2)
            
            for attempt in range(3):
                try:
                    self.client.futures_create_order(symbol=self.sym1, side=SIDE_BUY, type=ORDER_TYPE_MARKET, quantity=qty1)
                    log_success(self.logger, f"BUY {self.sym1} qty:{qty1}")
                    break
                except Exception as e:
                    log_error(self.logger, f"BUY {self.sym1} attempt {attempt+1} failed: {e}")
                    if attempt == 2:
                        return
            
            for attempt in range(3):
                try:
                    self.client.futures_create_order(symbol=self.sym2, side=SIDE_SELL, type=ORDER_TYPE_MARKET, quantity=qty2)
                    log_success(self.logger, f"SELL {self.sym2} qty:{qty2}")
                    break
                except Exception as e:
                    log_error(self.logger, f"SELL {self.sym2} attempt {attempt+1} failed: {e}")
                    if attempt == 2:
                        return
            
            print(f"BUY {self.sym1}, SELL {self.sym2}")
            trade_data = {"pair": self.pair, "action": "BUY", "mean": f"{self.mean.iloc[-1]:.5f}", "upper": f"{self.upper.iloc[-1]:.5f}", "lower": f"{self.lower.iloc[-1]:.5f}", "entry": f"{self.spread:.5f}"}
            self.save_trade_to_redis(trade_data)
            self.log_trade_to_csv(qty1, qty2)

        elif self.sngl == "SELL":
            existing_trade = self.rconn.get(f"trade:{self.pair}")
            if existing_trade:
                return
            qty1 = self.get_qty(self.sym1)
            qty2 = self.get_qty(self.sym2)
            
            for attempt in range(3):
                try:
                    self.client.futures_create_order(symbol=self.sym1, side=SIDE_SELL, type=ORDER_TYPE_MARKET, quantity=qty1)
                    log_success(self.logger, f"SELL {self.sym1} qty:{qty1}")
                    break
                except Exception as e:
                    log_error(self.logger, f"SELL {self.sym1} attempt {attempt+1} failed: {e}")
                    if attempt == 2:
                        return
            
            for attempt in range(3):
                try:
                    self.client.futures_create_order(symbol=self.sym2, side=SIDE_BUY, type=ORDER_TYPE_MARKET, quantity=qty2)
                    log_success(self.logger, f"BUY {self.sym2} qty:{qty2}")
                    break
                except Exception as e:
                    log_error(self.logger, f"BUY {self.sym2} attempt {attempt+1} failed: {e}")
                    if attempt == 2:
                        return
            
            print(f"SELL {self.sym1}, BUY {self.sym2}")
            trade_data = {"pair": self.pair, "action": "SELL", "mean": f"{self.mean.iloc[-1]:.5f}", "upper": f"{self.upper.iloc[-1]:.5f}", "lower": f"{self.lower.iloc[-1]:.5f}", "entry": f"{self.df['close'].iloc[-1]:.5f}"}
            self.save_trade_to_redis(trade_data)
            self.log_trade_to_csv(qty1, qty2)
