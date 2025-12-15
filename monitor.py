from binance.client import Client
from logger import setup_logger, log_success, log_error, log_warning
from config import Config, RedisConnection
from datetime import datetime
from data_class import StockData
import json
import os
import csv
import pandas as pd
import numpy as np
import asyncio
import time

class Positions(Config):
    def __init__(self):
        super().__init__()
        self.rconn = RedisConnection.get_instance()
        self.client = self.get_demo_client()
        self.logger = setup_logger('monitor_logger')
    
    def get_open_positions(self):
        positions = self.client.futures_position_information()
        open_pos = [p for p in positions if float(p['positionAmt']) != 0]
        return open_pos

    def log_exit_to_csv(self, pair, exit_spread):
        os.makedirs('log', exist_ok=True)
        with open('log/trade_history.csv', 'a', newline='') as f:
            csv.writer(f).writerow([datetime.now(), pair, "EXIT", None, None, None, None, 
                                    None, None, datetime.now(), exit_spread, "CLOSED"])

    def close_position(self, pair, exit_spread):
        s1, s2 = pair.split('_')
        positions = self.get_open_positions()
        deleted = False
        
        for pos in positions:
            symbol = pos['symbol']
            amt = float(pos['positionAmt'])
            if symbol not in [s1, s2] or amt == 0:
                continue
                
            side = 'SELL' if amt > 0 else 'BUY'
            qty = abs(amt)
            
            for attempt in range(3):
                try:
                    response = self.client.futures_create_order(symbol=symbol, side=side, type='MARKET', quantity=qty, newOrderRespType='RESULT')
                    if response['status'] == 'FILLED':
                        if not deleted:
                            self.log_exit_to_csv(pair, exit_spread)
                            self.rconn.delete(f"trade:{pair}")
                            deleted = True
                        log_success(self.logger, f"Position closed | {symbol}: {side} {qty}")
                        break
                    else:
                        log_warning(self.logger, f"Close order status: {response['status']}")
                except Exception as e:
                    log_error(self.logger, f"Close attempt {attempt+1} failed | {symbol}: {e}")
                    if attempt == 2:
                        log_error(self.logger, f"All retries failed for {symbol}")

class spd_ws(StockData, Positions):
    def __init__(self):
        StockData.__init__(self)
        Positions.__init__(self)
        self.pair = pd.read_csv('pair.csv')[['pair']].to_dict('records')
        self.rconn = RedisConnection.get_instance()
        self.logger = setup_logger('monitor_logger')
        self.pubsub = self.rconn.pubsub()
        self.last_prices = {}
        self.hedge_ratios = {}
        self.last_update_time = 0
        self.fetched_hd = False
        self.trades = {}

    async def on_message(self):
        while True:
            try:
                self.pubsub.psubscribe('stock:price:*', '__keyspace@0__:trade:*')
                for message in self.pubsub.listen():
                    if message['type'] != 'pmessage':
                        continue
                    channel = message['channel'].decode('utf-8')
                    if channel.startswith('stock:price:'):
                        self.last_prices[channel.split(':')[-1]] = float(message['data'].decode('utf-8'))
                        await self.calculate_spread()
                    elif channel.startswith('__keyspace@0__:trade:') and message['data'].decode('utf-8') == 'set':
                        pair = channel.split(':')[-1]
                        trade_data = self.rconn.get(f"trade:{pair}")
                        if trade_data:
                            try:
                                self.trades[pair] = json.loads(trade_data.decode('utf-8') if isinstance(trade_data, bytes) else trade_data)
                                df = self.get_spreads(pair).tail(1)
                                if len(df) > 0:
                                    self.hedge_ratios[pair] = float(df['hedge_ratio'].iloc[0])
                                log_success(self.logger, f"Trade detected | Pair: {pair} | Monitor: {datetime.now().strftime('%H:%M:%S')}")
                            except Exception as e:
                                log_error(self.logger, f"Trade load failed {pair}: {e}")
            except Exception as e:
                log_error(self.logger, f"Connection lost: {e}")
                await asyncio.sleep(5)
                self.pubsub = self.rconn.pubsub()

    async def calculate_spread(self):
        current_time = time.time()
        current_seconds = datetime.now().second
        if self.fetched_hd == False or current_seconds == 2 and (current_time - self.last_update_time) >= 60:
            self.fetched_hd = True
            for p in self.pair:
                pair = p['pair']
                df = self.get_spreads(pair).tail(1)
                if len(df) > 0:
                    self.hedge_ratios[pair] = float(df['hedge_ratio'].iloc[0])
                trade_data = self.rconn.get(f"trade:{pair}")
                self.trades[pair] = json.loads(trade_data) if trade_data else None
                # print(f"pair : {pair} trades : {self.trades[pair]}")
            self.last_update_time = current_time

        for p in self.pair:
            pair = p['pair']
            s1, s2 = pair.split('_')
            
            if s1 in self.last_prices and s2 in self.last_prices:
                hedge_ratio = self.hedge_ratios[pair]
                x = np.log(self.last_prices[s1])
                y = np.log(self.last_prices[s2])
                spd_close = y - (hedge_ratio * x)
                if self.trades.get(pair):
                    trade = self.trades[pair]
                    mean_price = float(trade['mean'])
                    action = trade['action']
                    if action == "BUY" and spd_close >= mean_price:
                        self.close_position(pair, spd_close)
                        del self.trades[pair]
                    elif action == "SELL" and spd_close <= mean_price:
                        self.close_position(pair, spd_close)   
                        del self.trades[pair]    

                    # print(f"{pair} | Spread: {spd_close:.5f} | mean_price: {mean_price} | Action: {action}")  
                    # print(f"trade {self.trades}")


# asyncio.run(spd_ws().on_message())

