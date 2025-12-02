from config import RedisConnection, Config
from datetime import datetime, timedelta
import polars as pl
import time
import os

class StockData:
    def __init__(self):
        self.redis = RedisConnection.get_instance()
    
    def _fetch_redis(self, symbol, start_time='-', end_time='+'):
        results = self.redis.ts().mrange(start_time, end_time, 
            filters=[f'symbol={symbol}', 'field=(open,close)'], with_labels=True)
        data = {list(item.values())[0][0]['field']: list(item.values())[0][1] for item in results}
        return pl.DataFrame({
            'date': [t + 1000 for t, v in data['open']],
            'open': [v for t, v in data['open']],
            'close': [v for t, v in data['close']]
        })
    
    def last_spd_data(self, pair):
        if not os.path.exists(f'data/spreads/{pair}.feather'):
            return None
        df = pl.read_ipc(f'data/spreads/{pair}.feather', memory_map=False)
        dt = df['date'].tail(1)
        return dt[0]

    def get(self, symbol, hours_back=24):
        historical = pl.read_ipc(f'data/{symbol}.feather', memory_map=False).select(['date', 'open', 'close'])
        historical = historical.with_columns(pl.col("date").cast(pl.Int64))

        cutoff = int((datetime.now() - timedelta(hours=hours_back)).timestamp() * 1000)
        latest = self._fetch_redis(symbol, start_time=cutoff)
        latest = latest.with_columns(pl.col("date").cast(pl.Int64))

        return (
            pl.concat([historical, latest])
            .unique(subset=['date'], keep='last')
            .sort('date')
        )

    def save_spreads(self, spreads, pair):
        os.makedirs('data/spreads/', exist_ok=True)
        file_path = f'data/spreads/{pair}.feather'
        spreads = spreads.filter(
            pl.col("close").is_finite() & pl.col("open").is_finite()
        )
        print("seving len",len(spreads))
        if os.path.exists(file_path):
            existing = pl.read_ipc(file_path, memory_map=False)
            spreads = pl.concat([existing, spreads])
        spreads.write_ipc(file_path, compression='zstd')
        return None

    def get_spreads(self, pair):
        return pl.read_ipc(f'data/spreads/{pair}.feather', memory_map=False)


# start = time.time()
# stock = StockData()
# df = stock.get('ETHUSDT')
# print(f"Fetch time: {(time.time() - start) * 1000:.2f} ms")  # ~50ms! 🚀
# print("df len", len(df))
# print(df.tail())

# while True:
#     now = datetime.now()
#     next_minute = (now - timedelta(minutes=now.minute % 1, seconds=now.second, microseconds=now.microsecond)) + timedelta(minutes=1, seconds=1)
#     print(f"Next minute: {next_minute} remaining: {next_minute - now}")
#     time.sleep((next_minute - now).total_seconds())

#     start = time.time()
#     df = stock.get('ETHUSDT')
#     print(f"Fetch time: {(time.time() - start) * 1000:.2f} ms")  # ~50ms! 🚀
#     print(f"Current time {datetime.now()}, df len", len(df))
#     print(df.tail())



# start = time.time()
# stock = StockData()
# df = stock.get_spreads('BTCUSDT_ETHUSDT')
# print(f"Fetch time: {(time.time() - start) * 1000:.2f} ms")  # ~50ms! 🚀
# print("df len", len(df))
# print(df.tail())