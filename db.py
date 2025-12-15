from datetime import datetime, timedelta
import sqlite3
import pandas as pd
from pytz import utc
import os
import json
import numpy as np
from config import RedisConnection

class DB():
    def __init__(self):
        self.conn = sqlite3.connect('crypto.db')
        self.cursor = self.conn.cursor()
        self.rconn = RedisConnection.get_instance()
    
    def save_data(self, chunk_data, symbol):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ohlc (symbol TEXT,date TEXT,open REAL,high REAL,low REAL,close REAL,volume INTEGER,PRIMARY KEY (symbol, date))
            ''')
        self.cursor.executemany('''
        INSERT OR IGNORE INTO ohlc (symbol, date, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', chunk_data)
        self.conn.commit()
        print(f"✅ Saved Api Data In db: {symbol}")

    def last_row(self, symbol): 
        query = "SELECT date FROM ohlc WHERE symbol=? ORDER BY date DESC LIMIT 1"
        self.cursor.execute(query, (symbol,))
        row = self.cursor.fetchone()
        if row:
            return row

    @staticmethod
    def all_data(symbol):
        conn = sqlite3.connect('crypto.db')
        cursor = conn.cursor()
        query = "SELECT symbol, date, open, close FROM ohlc WHERE symbol=? ORDER BY date"
        cursor.execute(query, (symbol,))
        rows = cursor.fetchall()
        return rows if rows else []

    def save_spreads(self, spreads, pair, live=False):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS spreads (symbol TEXT, date INTEGER, open REAL, close REAL,hedge_ratio REAL,PRIMARY KEY (symbol, date))
        ''')
        data = list(zip([pair] * len(spreads),spreads['date'].astype(int).tolist(),spreads['open'].astype(float).tolist(),spreads['close'].astype(float).tolist(),spreads['hedge_ratio'].astype(float).tolist()
        ))

        if live:
            key = f"spreads:{pair}"
            records = [json.dumps({ "date": int(r[0]),"open": float(r[1]),"close": float(r[2]),"hedge_ratio": float(r[3])}) for r in spreads]
            self.rconn.rpush(key, *records)
            
        # print("data", data[-5:])
        self.cursor.executemany('INSERT OR IGNORE INTO spreads VALUES (?, ?, ?, ?, ?)', data)
        self.conn.commit()
        # print(f"✅ Saved {len(data)} rows: {pair}")

    def spreads_last_row(self, symbol): 
        try:
            query = "SELECT date FROM spreads WHERE symbol=? ORDER BY date DESC LIMIT 1"
            self.cursor.execute(query, (symbol,))
            row = self.cursor.fetchone()
            if row:
                return int(row[0])
            return None
        except:
            return None
    
    def spreads_all_data(self, symbol, limit=None):
        if limit:
            query = "SELECT * FROM spreads WHERE symbol=? ORDER BY date DESC LIMIT ?"
            self.cursor.execute(query, (symbol, limit))
        else:
            query = "SELECT * FROM spreads WHERE symbol=? ORDER BY date"
            self.cursor.execute(query, (symbol,))
        return self.cursor.fetchall()