import sqlite3
import polars as pl
import json
import sys
from datetime import datetime
sys.path.append(r"G:\Automation\CRYPTO - v3")
from config import Config, RedisConnection


class HistoricalSpreadDB:
    def __init__(self):
        self.old_path = r"G:\Automation\CRYPTO - v3\crypto.db"
        self.new_path = r"G:\Automation\CRYPTO - v3\External\spreads.db"
    
    def get_old_data(self):
        conn = sqlite3.connect(self.old_path)
        df = pl.read_database("SELECT * FROM spreads", conn)
        conn.close()
        
        return df.with_columns([
            pl.from_epoch(pl.col("date") / 1000).dt.convert_time_zone("Asia/Kolkata").dt.replace_time_zone(None).dt.strftime("%Y-%m-%d %H:%M:%S").alias("date"),
            pl.max_horizontal("open", "close").alias("high"),
            pl.min_horizontal("open", "close").alias("low")
        ]).select(["symbol", "date", "open", "high", "low", "close", "hedge_ratio"])
    
    def create_table(self):
        conn = sqlite3.connect(self.new_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS spreads (
                symbol TEXT NOT NULL,
                date TEXT NOT NULL,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                hedge_ratio REAL,
                PRIMARY KEY (symbol, date)
            )
        """)
        conn.close()
    
    def save_data(self, df):
        df.write_database("spreads", f"sqlite:///{self.new_path}", if_table_exists="append")
    
    def run(self):
        self.create_table()
        df = self.get_old_data()
        self.save_data(df)
        print(f"✅ Saved {df.height} rows to spreads.db")


class RealtimeSpreadDB(Config):
    def __init__(self):
        super().__init__()
        self.db_path = r"G:\Automation\CRYPTO - v3\External\spreads.db"
        self.rconn = RedisConnection.get_instance()
        self.pubsub = self.rconn.pubsub()
        self.candles = {}
    
    def get_current_time(self):
        now = datetime.now()
        return now.replace(second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    
    def update_db(self, pair, candle):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            INSERT INTO spreads (symbol, date, open, high, low, close, hedge_ratio)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, date) DO UPDATE SET high=?, low=?, close=?, hedge_ratio=?
        """, (pair, candle['date'], candle['open'], candle['high'], candle['low'], 
              candle['close'], candle['hr'], candle['high'], candle['low'], 
              candle['close'], candle['hr']))
        conn.commit()
        conn.close()
    
    def process_realtime_data(self):
        self.pubsub.psubscribe('spread:price:*')
        
        for msg in self.pubsub.listen():
            if msg['type'] == 'pmessage':
                pair = msg['channel'].decode('utf-8').split(':')[-1]
                data = json.loads(msg['data'].decode('utf-8'))
                spread, hr = data['spread'], data['hedge_ratio']
                current_time = self.get_current_time()
                
                if pair not in self.candles or self.candles[pair]['date'] != current_time:
                    if pair in self.candles:
                        print(f"✅ Candle Complete | {pair} | {self.candles[pair]['date']} | O:{self.candles[pair]['open']:.6f} H:{self.candles[pair]['high']:.6f} L:{self.candles[pair]['low']:.6f} C:{self.candles[pair]['close']:.6f}")
                    self.candles[pair] = {'date': current_time, 'open': spread, 'high': spread, 'low': spread, 'close': spread, 'hr': hr}
                else:
                    c = self.candles[pair]
                    c['high'] = max(c['high'], spread)
                    c['low'] = min(c['low'], spread)
                    c['close'] = spread
                    c['hr'] = hr
                
                self.update_db(pair, self.candles[pair])

if __name__ == "__main__":
    # Historical data load karo
    # historical = HistoricalSpreadDB()
    # historical.run()

    realtime = RealtimeSpreadDB()
    realtime.process_realtime_data()