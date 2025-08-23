import os, json, uvloop, websockets

WS_URL = os.getenv("WS_URL", "wss://stream.binance.com:9443/ws/btcusdt@trade")

async def binance_trades(ws_url: str):
    async with websockets.connect(
        ws_url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
    ) as websocket:
        while True:
            try:
                data = await websocket.recv()     # 文字訊息
                trade = json.loads(data)          # 轉成 dict
                print(trade)                      # 先用 print，之後再換 logging
            except Exception as e:
                print("ERROR:", e)
                break

async def main():
    await binance_trades(WS_URL)

if __name__ == "__main__":
    uvloop.run(main())

