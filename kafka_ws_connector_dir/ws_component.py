import websockets, logging

log = logging.getLogger("ws")

async def binance_trades(ws_url: str):
    async with websockets.connect(
        ws_url,
        open_timeout=30,
        ping_interval=20,
        ping_timeout=20,
        close_timeout=10,) as ws:
        while True:
            try:
                data = await ws.recv()
                # log.debug(data)
                yield data

            except Exception as e:
                log.error("ERROR:", e)
                break
