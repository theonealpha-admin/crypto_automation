from binc import historical_data, webs
from data_class import StockData
import multiprocessing as mp
from multiprocessing import Pool
from db import DB
from rds import RedisData
from datetime import datetime, timedelta
from itertools import chain
import json
import numpy as np
import pandas as pd
import asyncio
import time
from config import Config, RedisConnection
from monitor import spd_ws
from sign_cls import execute
from spreads import Spreads

symbols = pd.read_csv('crypto.csv')[['symbol', 'exchange']].to_dict('records')
db = DB()
rconn = RedisConnection.get_instance()
def spd_sql_redis():
    pairs = pd.read_csv('pair.csv')['pair'].tolist()
    Conf = Config()
    rconn = RedisConnection.get_instance()
    lookback = (Conf.lookback + 5)
    for pair in pairs:
        key = f"spreads:{pair}"
        data = db.spreads_all_data(pair, limit=lookback)
        records = [json.dumps({"date": int(r[1]), "open": float(r[2]), "close": float(r[3]), "hedge_ratio": float(r[4])}) for r in data]
        rconn.delete(key)
        rconn.rpush(key, *records)
        # print("data", len(data))

def ws_process():
    red = RedisData()
    ws = webs()
    asyncio.run(ws.auto_close())

def read_all_data(): 
    sym = [item['symbol'] for item in symbols]
    with Pool(processes=10) as pool:
        results = pool.map(db.all_data, sym)
    all_data = list(chain.from_iterable(results))
    dtype = np.dtype([('symbol', 'U10'), ('date', 'i8'), ('open', 'f4'), ('close', 'f4')])
    temp_array = np.array(list(map(lambda d: (d[0], int(d[1]), d[2], d[3]), all_data)), dtype=dtype)
    temp_array.sort(order=['symbol', 'date'])
    return temp_array

def combine_redis_data(symbol, redis_data, stocks_df, pair):
    dtype = np.dtype([('symbol', 'U10'), ('date', 'i8'), ('open', 'f4'), ('close', 'f4')])
    sym_mask = stocks_df['symbol'] == symbol
    sym_data = stocks_df[sym_mask]
    redis_arr = redis_data.astype(dtype)
    if len(sym_data) == 0 and len(redis_arr) == 0:
        return np.array([], dtype=dtype)
    sym_data = sym_data.astype(dtype)
    combined = np.concatenate([sym_data, redis_arr])
    unique_combined = np.unique(combined, axis=0)
    return np.sort(unique_combined, order='date')

def spreads_cls(event, stocks_df):
    print("🏆 Starting Spreads Process...")
    spd = Spreads()
    pairs = pd.read_csv('pair.csv')['pair'].tolist()
    combined_cache = {}
    for pair in pairs:
        s1, s2, df1, df2 = spd.get_spd_data(pair)
        cdf1 = combine_redis_data(s1, df1, stocks_df, pair)
        cdf2 = combine_redis_data(s2, df2, stocks_df, pair)
        combined_cache[f"{pair}_1"] = cdf1
        combined_cache[f"{pair}_2"] = cdf2
        sp_df = spd.calculate_spread(s1, s2, cdf1, cdf2, pair)
    event.set()

    while True:
        event.clear()
        now = datetime.now()
        next_minute = (now - timedelta(minutes=now.minute % 1, seconds=now.second, microseconds=now.microsecond)) + timedelta(minutes=1, seconds=1)
        print(f"Next minute for spreads: {next_minute} remaining: {next_minute - now}")
        time.sleep((next_minute - now).total_seconds())
        start = time.time()
        
        for pair in pairs:
            s1, s2, df1, df2 = spd.get_spd_data(pair)
            cdf1 = np.concatenate([combined_cache[f"{pair}_1"], df1[-1:]])
            cdf2 = np.concatenate([combined_cache[f"{pair}_2"], df2[-1:]])
            combined_cache[f"{pair}_1"] = cdf1
            combined_cache[f"{pair}_2"] = cdf2
            sp_df = spd.calculate_spread(s1, s2, cdf1, cdf2, pair, live=True)
        print(f"⏱️ Spreads loop total: {time.time()-start:.2f}s")
        rconn.publish('spreads:updated', 'done')
        event.set()

def execute_cls(event):
    pairs = pd.read_csv('pair.csv')['pair'].tolist()
    for pair in pairs:
        td = execute(pair)
        td.trade()
    while True:
        event.wait()
        start = time.time()
        for pair in pairs:
            td = execute(pair)
            td.trade()
        print(f"⏱️ Execute Signals loop total: {time.time()-start:.2f}s")

def monitor_cls():
    asyncio.run(spd_ws().on_message())

if __name__ == '__main__':
    mp.set_start_method('spawn')
    init_done = mp.Event()
    # ##Websocket class
    p1 = mp.Process(target=ws_process)
    p1.start()

    historical_data = historical_data()
    ohlcv = historical_data.ohlcv()
    print("🏆 Completed OHLCV Downloader")
    stocks_df = read_all_data()
    
    p2 = mp.Process(target=spreads_cls, args=(init_done, stocks_df))
    p2.start()
    init_done.wait()
    print("🏆 p2 initialization complete (Spreads ✅)! Starting p3 and p4...")
    spd_sql_redis()
    p3 = mp.Process(target=execute_cls, args=(init_done,))
    p4 = mp.Process(target=monitor_cls)
    p3.start()
    p4.start()
    