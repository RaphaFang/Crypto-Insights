from confluent_kafka import Producer

def make_producer(bootstrap: str = "localhost:9092") -> Producer:
    return Producer({
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "compression.type": "zstd",
        "linger.ms": 20,
        "client.id": "ws-connector",
    })

def send(p: Producer, value: bytes, topic: str = "btc_raw", key: bytes | None = None):
    p.produce(topic=topic, value=value, key=key)
    p.poll(0)

def close(p: Producer, timeout_sec: float = 5.0):
    p.flush(timeout=timeout_sec)