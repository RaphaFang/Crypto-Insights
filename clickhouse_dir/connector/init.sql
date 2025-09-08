-- 1) 建 DB
CREATE DATABASE IF NOT EXISTS agg;

-- 2) 最終表：解析後的結構化資料
CREATE TABLE IF NOT EXISTS agg.vwap (
  symbol LowCardinality(String),
  window_start  DateTime64(3),
  window_end    DateTime64(3),
  vwap          Decimal(18,6),
  qtySum        UInt64,
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

-- 3) 原始落地表：存 JSON 字串
CREATE TABLE IF NOT EXISTS agg.vwap_raw (
  raw String,
  ingested_at DateTime64(3) DEFAULT now64()
) ENGINE = MergeTree
ORDER BY ingested_at;

-- 4) Kafka 表（RawBLOB，專門存原文）
DROP TABLE IF EXISTS agg.vwap_queue_raw;
CREATE TABLE agg.vwap_queue_raw (
  raw String
)
ENGINE = Kafka
SETTINGS
  kafka_broker_list = 'kafka:9092',
  kafka_topic_list  = 'agg.vwap',
  kafka_group_name  = 'ch_vwap_init',   -- ⚠️ 新 group，避免 offset 卡死
  kafka_format      = 'RawBLOB';

-- 5) MV：Kafka → 原始表
DROP TABLE IF EXISTS agg.vwap_mv_raw;
CREATE MATERIALIZED VIEW agg.vwap_mv_raw
TO agg.vwap_raw AS
SELECT raw FROM agg.vwap_queue_raw;

-- 6) MV：raw → vwap（解析 JSON + 修正小數）
DROP TABLE IF EXISTS agg.vwap_parse_mv;
CREATE MATERIALIZED VIEW agg.vwap_parse_mv
TO agg.vwap AS
SELECT
  JSONExtractString(j, 'symbol')                                    AS symbol,
  toDateTime64(JSONExtractUInt(j, 'windowStart') / 1000.0, 3)       AS window_start,
  toDateTime64(JSONExtractUInt(j, 'windowEnd')   / 1000.0, 3)       AS window_end,
  toDecimal64(JSONExtractFloat(j, 'vwap'), 6)                        AS vwap,
  JSONExtractUInt(j, 'qtySum')                                       AS qtySum,
  JSONExtractUInt(j, 'rawDataCount')                                 AS rawDataCount,
  JSONExtractUInt(j, 'secFilled')                                    AS secFilled,
  toDateTime64(JSONExtractUInt(j, 'eventTimeMin') / 1000.0, 3)       AS eventTimeMin,
  toDateTime64(JSONExtractUInt(j, 'eventTimeMax') / 1000.0, 3)       AS eventTimeMax,
  toDateTime64(JSONExtractUInt(j, 'emitTime')      / 1000.0, 3)      AS emitTime,
  JSONExtractUInt(j, 'eventToEmitLatencyMs')                         AS eventToEmitLatencyMs,
  JSONExtractUInt(j, 'firstIngestToEmitLatencyMs')                   AS firstIngestToEmitLatencyMs,
  JSONExtractUInt(j, 'lastIngestToEmitLatencyMs')                    AS lastIngestToEmitLatencyMs
FROM
(
  -- 修正 .123 → 0.123（RE2 不支援 lookbehind，用分組處理）
  SELECT replaceRegexpAll(raw, '(^|[^0-9])\\.(\\d)', '\\10.\\2') AS j
  FROM agg.vwap_raw
);
