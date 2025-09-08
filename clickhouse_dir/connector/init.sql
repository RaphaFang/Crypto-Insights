-- ClickHouse 的物件是 database.table, 很搞笑的會撞名, 如果我的名稱沿用 kafka topic, "agg.vwap"
CREATE DATABASE IF NOT EXISTS agg;

CREATE TABLE IF NOT EXISTS agg.vwap (
  symbol LowCardinality(String),

  window_start  DateTime64(3),
  window_end    DateTime64(3),

  vwap  Decimal(18,6),
  qtySum       UInt64,
  rawDataCount UInt32,
  secFilled    UInt32,

  eventTimeMin DateTime64(3),
  eventTimeMax DateTime64(3),
  emitTime     DateTime64(3),
  eventToEmitLatencyMs        UInt32,
  firstIngestToEmitLatencyMs  UInt32,
  lastIngestToEmitLatencyMs   UInt32,

  version UInt64 DEFAULT toUnixTimestamp64Milli(now64())
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toDate(window_end)
ORDER BY (symbol, window_end)
SETTINGS index_granularity = 8192;
