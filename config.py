from binance import Client, ThreadedWebsocketManager
import json
import os
import redis

class Config:
    def __init__(self):
        self.interval = "1m"
        self.api_key = 'jrXkGM33TgGgeTuCP8a8Zwfo4YuItLYlIJ5z7IILdAM5lUNHGxyxuOv06c02wNqA'
        self.api_secret = '08bAflWviEUI0mxqTeDPWfiK0J3YzNtbB2vZMi6luFANnq18br945PA9SxFWZy6E'
        # self.api_key = 'o38amEHjMlQmelKOTSBdh7u1q4YxROtTlpdeNa16eeWHDCPg7ETWuQ9q5y6IEJlx'
        # self.api_secret = 'InOIa6gGJVjQVIbvNfnxv0uJp34V6kkpo1TGVbX8BjTigoYOTGZOMIPrlLUfTDiI'

    def binance_client(self):
        client = Client(self.api_key, self.api_secret, testnet=True)
        bm = ThreadedWebsocketManager(api_key=self.api_key, api_secret=self.api_secret, testnet=True)
        return client, bm



class RedisConnection:
    _instance = None

    @staticmethod
    def get_instance():
        if RedisConnection._instance is None:
            RedisConnection._instance = redis.Redis(host='localhost', port=6380, db=0)
        return RedisConnection._instance
