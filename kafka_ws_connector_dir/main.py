import os, uvloop
from k_producer import make_producer, send, close
from ws_component import binance_trades
from logger import setup_logger



WS_URL = os.getenv("WS_URL", "wss://stream.binance.com:9443/ws/btcusdt@trade")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ws_raw")
LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


async def main():
    p = make_producer()
    try:
        async for msg in binance_trades(WS_URL):
            send(p=p, topic="btc_raw", value=msg.encode("utf-8"))
    finally:
        close(p)

if __name__ == "__main__":
    setup_logger(LEVEL)
    uvloop.run(main())