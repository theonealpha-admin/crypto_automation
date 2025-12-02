from binance.client import Client
from config import Config, RedisConnection
from db import DB
from datetime import datetime, timedelta
import pandas as pd
import polars as pl
import asyncio
import time

class historical_data(Config, DB):
    def __init__(self):
        super().__init__()
        self.Cnfg=Config()
        self.db=DB()
        self.base_start = 1746057660000 # 2025-05-01T09:15:00.000Z
        self.end_date = int(datetime.now().timestamp() * 1000)
        self.symbols = pd.read_csv('crypto.csv')[['symbol', 'exchange']].to_dict('records')

    def _get_historical_data(self, symbol):
        client, _ = self.Cnfg.binance_client()
        start = self.db.last_row(symbol['symbol'])
        start = self.base_start if start is None else int(start[0])
        while start < self.end_date:
            chunk_end = min(start + 28800000, self.end_date)
            candles = client.get_historical_klines(symbol['symbol'], Client.KLINE_INTERVAL_1MINUTE, start, chunk_end)
            
            rows = [(symbol['symbol'], int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])) 
                    for c in candles if c]
            if rows:
                self.db.save_data(rows, symbol['symbol'])
            start = chunk_end + 60000
            time.sleep(0.3)

    def ohlcv(self):
        print("🚀 Starting OHLCV Downloader...")
        for sym in self.symbols:
            self._get_historical_data(sym)


class webs(Config, DB): 
    def __init__(self): 
        super().__init__() 
        self.Cnfg=Config() 
        self.db=DB() 
        self.symbols = pd.read_csv('crypto.csv')['symbol'].tolist()  
        self.rconn = RedisConnection.get_instance() 
        self.client, self.bm = self.Cnfg.binance_client()
 
    def on_message(self, msg):
        data = msg.get('data', msg)
        if 'c' not in data:
            return
        ts_key = f"stock:ticks:{data['s']}"
        self.rconn.ts().add(ts_key, '*', float(data['c']))

        channel = f"stock:price:{data['s']}"
        self.rconn.publish(channel, float(data['c']))
        print(f"🔥 {data['s']}: {data['c']}")
         
    def auto_close(self):
        self.bm.start()
        streams = [f"{symbol.lower()}@ticker" for symbol in self.symbols]
        self.bm.start_multiplex_socket(callback=self.on_message, streams=streams)
        print("🚀 Binance WebSocket connected!")
        
        import time
        while True:
            time.sleep(30)