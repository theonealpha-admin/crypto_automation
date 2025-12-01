from binc import historical_data, webs
from data_class import StockData
import multiprocessing as mp
from multiprocessing import Pool
from db import process_symbol
from rds import RedisData
import pandas as pd
import asyncio
import time

symbols = pd.read_csv('crypto.csv')[['symbol', 'exchange']].to_dict('records')

def ws_process():
    red = RedisData()
    ws = webs()
    asyncio.run(ws.auto_close())

def write_file():
    sym = [item['symbol'] for item in symbols]
    with Pool(processes=10) as pool:
        pool.map(process_symbol, sym)

if __name__ == '__main__':
    mp.set_start_method('spawn')

    # #Websocket class
    p1 = mp.Process(target=ws_process)
    p1.start()

    # historical_data = historical_data()
    # ohlcv = historical_data.ohlcv()
    # print("🏆 Completed OHLCV Downloader")

    # # sql to feather
    # write_file()
    # print("🏆 Completed writing feather files")


#data class
# StockData()
# df = stock.get('GOLD25DECFUT')