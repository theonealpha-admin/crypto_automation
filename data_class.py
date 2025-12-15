from config import RedisConnection, Config
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import time
import os
import json

class StockData:
    def __init__(self):
        self.redis = RedisConnection.get_instance()
    
    def _fetch_redis(self, symbol, start_time='-', end_time='+'):
        results = self.redis.ts().mrange(start_time, end_time, 
            filters=[f'symbol={symbol}', 'field=(open,close)'], with_labels=True)
        data = {list(item.values())[0][0]['field']: list(item.values())[0][1] for item in results}
        
        dates = [t + 1000 for t, v in data['open']]
        opens = [v for t, v in data['open']]
        closes = [v for t, v in data['close']]
        
        dtype = np.dtype([('symbol', 'U10'), ('date', 'i8'), ('open', 'f4'), ('close', 'f4')])
        symbols = [symbol] * len(dates)
        return np.array(list(zip(symbols, dates, opens, closes)), dtype=dtype)
    
    def get_spreads(self, pair):
        key = f"spreads:{pair}"
        data = self.redis.lrange(key, 0, -1)
        df = pd.DataFrame([json.loads(x) for x in data])
        df = df.drop_duplicates(subset='date', keep='last')
        df = df.sort_values('date')
        return df

    def get(self, symbol, hours_back=24):
        cutoff = int((datetime.now() - timedelta(hours=hours_back)).timestamp() * 1000)
        latest = self._fetch_redis(symbol, start_time=cutoff)
        return np.sort(latest, order='date')
