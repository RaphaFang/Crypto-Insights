import websockets, logging

log = logging.getLogger("ws")

async def binance_trades(ws_url: str):
    async with websockets.connect(
        ws_url,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,
    ) as websocket:
        while True:
            try:
                data = await websocket.recv()
                log.info(data.encode("utf-8"))
            except Exception as e:
                log.error("ERROR:", e)
                break



