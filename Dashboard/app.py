import json
import asyncio
import numpy as np
import redis
import redis.asyncio as aioredis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Set, List

# --- Configuration ---
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Sync Redis (API और Historical Data Fetching के लिए)
redis_sync = redis.Redis(host='localhost', port=6380, decode_responses=True)

# Async Redis URL (WebSocket Listener के लिए)
REDIS_URL = "redis://localhost:6380"

# --- Global State ---
active_connections: Dict[str, Set[WebSocket]] = {}
hedge_ratios: Dict[str, float] = {} # { pair: last_known_beta }
last_prices: Dict[str, float] = {}
pair_listeners: Dict[str, asyncio.Task] = {}

LOOKBACK = 20
STD_MULTIPLIER = 2

# --- Helper Functions (Math & Data) ---

def calculate_bands(closes: List[float], lookback=LOOKBACK):
    """Bollinger Bands calculation for historical data"""
    if len(closes) < lookback:
        return [], [], []
    
    closes_array = np.array(closes)
    mean_vals = [None] * (lookback - 1)
    upper_vals = [None] * (lookback - 1)
    lower_vals = [None] * (lookback - 1)
    
    for i in range(lookback - 1, len(closes_array)):
        window = closes_array[i - lookback + 1:i + 1]
        mean = np.mean(window)
        std = np.std(window)
        
        mean_vals.append(float(mean))
        upper_vals.append(float(mean + STD_MULTIPLIER * std))
        lower_vals.append(float(mean - STD_MULTIPLIER * std))
        
    return mean_vals, upper_vals, lower_vals

def get_latest_hedge_ratio(pair: str) -> float:
    """
    CORRECTED LOGIC: 
    Reads the hedge ratio from the last saved spread candle, 
    matching the background process's source.
    """
    try:
        # Fetch the very last spread candle data (one element)
        raw_last_spread = redis_sync.lindex(f"spreads:{pair}", -1)
        if raw_last_spread:
            last_spread_data = json.loads(raw_last_spread)
            # 'hedge_ratio' key का उपयोग किया जाता है जो 'calculate_spread' फंक्शन में सेव हुआ था
            return float(last_spread_data.get('hedge_ratio', 1.0)) 
    except Exception as e:
        print(f"Error fetching latest hedge ratio for {pair}: {e}")
    return 1.0 # Default to 1.0 if any error occurs

def get_chart_data(pair: str):
    """Fetch full historical chart data from Redis (Sync)"""
    # 1. Fetch Spread History
    raw_spreads = redis_sync.lrange(f"spreads:{pair}", -300, -1)
    if not raw_spreads:
        return {'dates': [], 'closes': [], 'mean': [], 'upper': [], 'lower': [], 'trades': []}

    parsed_spreads = [json.loads(x) for x in raw_spreads]
    dates = [x['date'] for x in parsed_spreads]
    closes = [x['close'] for x in parsed_spreads]

    # 2. Calculate Bands
    mean, upper, lower = calculate_bands(closes)

    # 3. Fetch Trades
    trades = []
    trade_data = redis_sync.get(f"trades:{pair}")
    if trade_data:
        try:
            # Note: Assuming trade data is a JSON list of trade objects
            trade_list = json.loads(trade_data) 
            trades = [{
                'date': t.get('date'),
                'spread': float(t.get('entry', 0)),
                'action': t.get('action', 'BUY'),
                'pnl': t.get('pnl', 0)
            } for t in trade_list]
        except Exception as e:
            print(f"Error parsing trades: {e}")

    return {
        'dates': dates,
        'closes': closes,
        'mean': mean,
        'upper': upper,
        'lower': lower,
        'trades': trades
    }

def get_dynamic_pairs():
    """Scan Redis for available pairs"""
    keys = redis_sync.keys("spreads:*")
    return [k.replace("spreads:", "") for k in keys]

# --- WebSocket & Async Logic ---

async def broadcast_to_pair(pair: str, message: dict):
    """Send message to all clients watching a specific pair"""
    if pair not in active_connections:
        return

    dead_connections = set()
    for ws in active_connections[pair]:
        try:
            await ws.send_json(message)
        except Exception:
            dead_connections.add(ws)
    
    # Cleanup broken connections
    for ws in dead_connections:
        active_connections[pair].discard(ws)

async def redis_listener(pair: str):
    """Background Task: Listens to price updates and broadcasts live spread."""
    print(f"🎧 Started Listener for {pair}")
    
    try:
        s1, s2 = pair.split('_')
        
        r = await aioredis.from_url(REDIS_URL, decode_responses=True)
        pubsub = r.pubsub()
        
        await pubsub.subscribe(f"stock:price:{s1}", f"stock:price:{s2}", "spreads:updated")

        # 🎯 FIX APPLIED HERE: Initial Hedge Ratio Fetch now uses the correct source
        hedge_ratios[pair] = get_latest_hedge_ratio(pair)

        async for message in pubsub.listen():
            if message['type'] != 'message':
                continue

            channel = message['channel']
            data = message['data']

            # --- CASE 1: Live Price Update (Calculate Spread) ---
            if channel.startswith("stock:price:"):
                symbol = channel.split(":")[-1]
                
                try:
                    price = float(data)
                    last_prices[symbol] = price
                    
                    if s1 in last_prices and s2 in last_prices:
                        p1 = last_prices[s1]
                        p2 = last_prices[s2]
                        # Use the cached, latest ratio
                        ratio = hedge_ratios.get(pair, 1.0) 
                        
                        # Calculation: Spread = log(P2) - ratio * log(P1)
                        spread_val = np.log(p2) - (ratio * np.log(p1))
                        
                        await broadcast_to_pair(pair, {
                            'type': 'live_spread',
                            'spread': float(spread_val)
                        })
                except Exception as e:
                    # Low price/zero price can cause log error
                    print(f"⚠️ Live Calc Error: {e}")

            # --- CASE 2: New Candle / Historical Update ---
            elif channel == "spreads:updated":
                # 🎯 FIX APPLIED HERE: Refresh Hedge Ratio from the correct source
                hedge_ratios[pair] = get_latest_hedge_ratio(pair)
                
                # Fetch full chart data 
                chart_data = get_chart_data(pair)
                
                await broadcast_to_pair(pair, {
                    'type': 'chart_update',
                    'data': chart_data
                })

    except asyncio.CancelledError:
        print(f"🛑 Listener stopped for {pair}")
    except Exception as e:
        print(f"❌ Listener Error {pair}: {e}")
    finally:
        await pubsub.close()
        await r.close()

# --- API Routes ---

@app.websocket("/ws/{pair}")
async def websocket_endpoint(websocket: WebSocket, pair: str):
    await websocket.accept()
    
    # 1. Add to active connections
    if pair not in active_connections:
        active_connections[pair] = set()
    active_connections[pair].add(websocket)
    
    # 2. Start Redis Listener for this pair if not running
    if pair not in pair_listeners or pair_listeners[pair].done():
        pair_listeners[pair] = asyncio.create_task(redis_listener(pair))

    try:
        # 3. Send Initial Historical Data immediately
        await websocket.send_json({
            'type': 'initial_data',
            'data': get_chart_data(pair)
        })
        
        # 4. Keep Connection Alive
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"⚠️ WS Error: {e}")
    finally:
        # 5. Cleanup
        if pair in active_connections:
            active_connections[pair].discard(websocket)
            if not active_connections[pair]:
                del active_connections[pair]
                if pair in pair_listeners:
                    pair_listeners[pair].cancel()
                    del pair_listeners[pair]

@app.get("/api/pairs")
def get_pairs():
    return {"pairs": get_dynamic_pairs()}

# Note: /api/stats/{pair} is removed as we no longer store zscore in Redis sync.
# You can re-add it if your background process updates a 'zscore:{pair}' key.

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8005)