FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    confluent-kafka==2.11.1 \
    websockets==15.0.1 \
    uvloop==0.21.0 \
    logging==0.4.9.6

WORKDIR /app
COPY . /app/

# ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "main.py"]