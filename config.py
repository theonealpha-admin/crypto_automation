from binance import Client, ThreadedWebsocketManager
import json
import os
import redis

class Config:
    def __init__(self):
        self.interval = "1m" #time interval for OHLCV
        self.fund=4999 #account balance
        self.window=120 #for spreads
        self.lookback=20 #for zscore
        self.std=2 #for zscore
        self.api_key = 'jrXkGM33TgGgeTuCP8a8Zwfo4YuItLYlIJ5z7IILdAM5lUNHGxyxuOv06c02wNqA'
        self.api_secret = '08bAflWviEUI0mxqTeDPWfiK0J3YzNtbB2vZMi6luFANnq18br945PA9SxFWZy6E'
        
        # Demo/Testnet credentials
        self.demo_api_key = 'xD7T3AtRLleOfXgCtWDVbZ7LphRCaV17FtThGsWeWXJ0P30wER4MeVfCJdcXYGP1'
        self.demo_api_secret = 'VWBbrW4C5oxRRqLlpo3tv2pyhX0HA8t0cuy8vN24kvr7GAYNKbRkczej1d92Zunr'

    def binance_client(self):
        client = Client(self.api_key, self.api_secret)
        bm = ThreadedWebsocketManager(api_key=self.api_key, api_secret=self.api_secret)
        return client, bm

    def get_demo_client(self):
        client = Client(self.demo_api_key, self.demo_api_secret)
        client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'
        return client

class RedisConnection:
    _instance = None

    @staticmethod
    def get_instance():
        if RedisConnection._instance is None:
            RedisConnection._instance = redis.Redis(host='localhost', port=6380, db=0)
        return RedisConnection._instance
