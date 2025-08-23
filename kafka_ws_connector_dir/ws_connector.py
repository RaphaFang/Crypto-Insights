import os, uvloop, websockets, json, signal
from kafka import KafkaProducer

WS_URL = os.getenv("WS_URL", "wss://stream.binance.com:9443/ws/btcusdt@trade")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ws_raw")

async def binance_trades():
    url = WS_URL

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


if __name__ == "__main__":
    uvloop.run(main())