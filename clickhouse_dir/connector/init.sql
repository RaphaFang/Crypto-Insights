CREATE DATABASE IF NOT EXISTS agg;

-- 如果格式錯誤，再來啟動
DROP TABLE IF EXISTS agg.vwap;
DROP TABLE IF EXISTS agg.vwap_queue;
DROP TABLE IF EXISTS agg.vwap_mv;

CREATE TABLE IF NOT EXISTS agg.vwap (
  symbol LowCardinality(String),
  window_start  DateTime64(3),
  window_end    DateTime64(3),
  vwap          Decimal(18,6),
  qtySum        Decimal(18,8),
  rawDataCount  UInt32,
  secFilled     UInt32,
  eventTimeMin  DateTime64(3),
  eventTimeMax  DateTime64(3),
  emitTime      DateTime64(3),
  eventToEmitLatencyMs       UInt32,
  firstIngestToEmitLatencyMs UInt32,
  lastIngestToEmitLatencyMs  UInt32
) ENGINE = MergeTree
PARTITION BY toDate(window_end)
ORDER BY (symbol, window_end);

CREATE TABLE agg.vwap_queue
(
  symbol String,
  ma Double,
  windowStart UInt64,
  windowEnd   UInt64,
  rawDataCount UInt32,
  qtySum Decimal(18,8),
  vwap Decimal(18,6),
  secFilled UInt32,
  eventTimeMin UInt64,
  eventTimeMax UInt64,
  emitTime UInt64,
  eventToEmitLatencyMs UInt32,
  firstIngestToEmitLatencyMs UInt32,
  lastIngestToEmitLatencyMs  UInt32
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list  = 'agg.vwap',
  kafka_group_name  = 'ch_vwap_direct',
  kafka_format      = 'JSONEachRow',
  input_format_skip_unknown_fields = 1,
  kafka_skip_broken_messages = 100;

CREATE MATERIALIZED VIEW agg.vwap_mv
TO agg.vwap AS
SELECT
  symbol,
  toDateTime64(windowStart/1000.0, 3) AS window_start,
  toDateTime64(windowEnd/1000.0,   3) AS window_end,
  toDecimal64(vwap,   6)              AS vwap,
  toDecimal64(qtySum, 8)              AS qtySum,
  rawDataCount,
  secFilled,
  toDateTime64(eventTimeMin/1000.0, 3) AS eventTimeMin,
  toDateTime64(eventTimeMax/1000.0, 3) AS eventTimeMax,
  toDateTime64(emitTime/1000.0,     3) AS emitTime,
  eventToEmitLatencyMs,
  firstIngestToEmitLatencyMs,
  lastIngestToEmitLatencyMs
FROM agg.vwap_queue;