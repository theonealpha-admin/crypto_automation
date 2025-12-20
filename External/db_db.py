import sqlite3
import polars as pl
import sys
sys.path.append(r"G:\Automation\CRYPTO - v3")
from config import Config


class HistoricalSpreadDB:
    def __init__(self):
        self.old_path = r"G:\Automation\CRYPTO - v3\crypto.db"
        self.new_path = r"G:\Automation\CRYPTO - v3\External\spreads.db"
    
    def get_old_data(self):
        conn = sqlite3.connect(self.old_path)
        df = pl.read_database("SELECT * FROM spreads", conn)
        conn.close()
        
        return df.with_columns([
            pl.from_epoch(pl.col("date") / 1000).dt.replace_time_zone(None).dt.strftime("%Y-%m-%d %H:%M:%S").alias("date"),
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
        self.new_path = r"G:\Automation\CRYPTO - v3\External\spreads.db"
    
    def process_realtime_data(self):
        # Realtime data processing logic yahan aayega
        pass

if __name__ == "__main__":
    # Historical data load karo
    historical = HistoricalSpreadDB()
    historical.run()