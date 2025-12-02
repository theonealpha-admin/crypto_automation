from binance.client import Client

class Positions:
    def __init__(self):
        api_key = 'xD7T3AtRLleOfXgCtWDVbZ7LphRCaV17FtThGsWeWXJ0P30wER4MeVfCJdcXYGP1'
        api_secret = 'VWBbrW4C5oxRRqLlpo3tv2pyhX0HA8t0cuy8vN24kvr7GAYNKbRkczej1d92Zunr'
        
        self.client = Client(api_key, api_secret)
        self.client.FUTURES_URL = 'https://testnet.binancefuture.com/fapi'
    
    def get_open_positions(self):
        positions = self.client.futures_position_information()
        open_pos = [p for p in positions if float(p['positionAmt']) != 0]
        return open_pos
    
    def show_positions(self):
        positions = self.get_open_positions()
        
        if not positions:
            print("No open positions")
            return
        
        for pos in positions:
            print(f"Symbol: {pos['symbol']}")
            print(f"Unrealized PNL: {pos['unRealizedProfit']}")
            print("-" * 40)


if __name__ == "__main__":
    p = Positions()
    p.show_positions()