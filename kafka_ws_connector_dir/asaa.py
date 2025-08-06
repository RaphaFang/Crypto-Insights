import asyncio
import websockets
import json

async def listen_binance_trades():
    url = "wss://stream.binance.com:9443/ws/btcusdt@trade"
    async with websockets.connect(url) as websocket:
        while True:
            try:
                data = await websocket.recv()
                trade = json.loads(data)
                print(trade)
                # price = trade["p"]    # 成交價格
                # quantity = trade["q"] # 成交數量
                # timestamp = trade["T"]
                # print(f"[成交時間: {timestamp}] 價格: {price} 數量: {quantity}")
            except Exception as e:
                print("ERROR:", e)
                break

asyncio.run(listen_binance_trades())
