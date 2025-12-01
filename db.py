from datetime import datetime, timedelta
import sqlite3
import pandas as pd
from pytz import utc
import os
import polars as pl
from config import RedisConnection

class DB():
    def __init__(self):
        self.conn = sqlite3.connect('crypto.db')
        self.cursor = self.conn.cursor()
        self.rconn = RedisConnection.get_instance()
    
    def save_data(self, chunk_data, symbol): #saving data to sqlite
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ohlc (symbol TEXT,date TEXT,open REAL,high REAL,low REAL,close REAL,volume INTEGER,PRIMARY KEY (symbol, date))
            ''')
        self.cursor.executemany('''
        INSERT OR IGNORE INTO ohlc (symbol, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', chunk_data)
        self.conn.commit()
        print(f"✅ Saved Api Data In db: {symbol}")

    def last_row(self, symbol): #getting last row from sqlite
        try:
            query = "SELECT date FROM ohlc WHERE symbol=? ORDER BY date DESC LIMIT 1"
            self.cursor.execute(query, (symbol,))
            row = self.cursor.fetchone()
            if row:
                return row
        except Exception as e:
            print(f"X Db Not Found")
            return None
        return None

    def all_data(self, symbol): #getting all data from sqlite
        try:
            query = "SELECT * FROM ohlc WHERE symbol=? ORDER BY date"
            self.cursor.execute(query, (symbol,))
            row = self.cursor.fetchall()
            if row:
                return row
        except Exception as e:
            print(f"X Db Not Found")
            return None
        return None
        
    def sql_to_feather(self, symbol): #using this sql to feather
        df = self.all_data(symbol)
        columns = ['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']
        dd = pl.DataFrame(df, schema=columns, orient='row')
        # print("dd", dd)
        os.makedirs('data', exist_ok=True)
        dd.write_ipc(f'data/{symbol}.feather', compression='lz4')
        print(f"✅ Saved: data/{symbol}.feather ({len(dd)} rows)")
        return dd

def process_symbol(symbol):
    db = DB()
    db.sql_to_feather(symbol)