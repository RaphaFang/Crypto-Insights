FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    confluent-kafka==2.6.0 \
    websockets==12.0 \
    uvloop==0.20.0

WORKDIR /app
COPY app/ /app/

ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "ws_connector"]