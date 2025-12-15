from config import RedisConnection, Config
import redis
import pandas as pd

class RedisData(Config):
    def __init__(self):
        super().__init__()
        self.rconn = RedisConnection.get_instance()
        self.intv = "1minute" if self.interval == "minute" else self.interval
        self.symbols = pd.read_csv('crypto.csv')[['symbol', 'exchange']].to_dict('records')
        self.create_ohlc_rules(self.symbols)

    def create_ohlc_rules(self, symbols):
        ohlc_fields = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
        intv= int(self.intv[0])
        bucket_size_msec = intv * 60 * 1000
        for symbol in symbols:
            try:
                source_key = f"stock:ticks:{symbol['symbol']}"
                self.rconn.ts().create(source_key)
                for field, agg_type in ohlc_fields.items():
                    dest_key = f"stock:{symbol['symbol']}:{field}"
                    self.rconn.ts().create(dest_key, labels={"symbol": symbol['symbol'], "field": field})
                    self.rconn.ts().createrule(
                        source_key,
                        dest_key,
                        aggregation_type=agg_type,
                        bucket_size_msec=bucket_size_msec,
                        align_timestamp=bucket_size_msec - 1000
                    )
                    print(f" ✓ {field}: {source_key} → {dest_key} (agg: {agg_type})")
            except Exception as e:
                print(f"Error for symbol {symbol['symbol']}: {e}")

# red = RedisData()