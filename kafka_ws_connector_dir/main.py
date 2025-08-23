import os, uvloop

WS_URL = os.getenv("WS_URL", "wss://stream.binance.com:9443/ws/btcusdt@trade")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "ws_raw")


if __name__ == "__main__":
    uvloop.run(main())