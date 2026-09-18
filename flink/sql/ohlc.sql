SET 'pipeline.name' = 'ohlc-1m';
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.source.idle-timeout' = '15s';

CREATE TABLE ticks (
  symbol          STRING,
  trade_id        BIGINT,
  price           DECIMAL(18, 8),
  qty             DECIMAL(18, 8),
  event_time_ms   BIGINT,
  ingest_time_ms  BIGINT,
  is_buyer_maker  BOOLEAN,
  `source`        STRING,
  event_time AS TO_TIMESTAMP_LTZ(event_time_ms, 3),
  WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ticks',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'properties.group.id' = 'flink-ohlc',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE candles (
  symbol        STRING,
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  `open`        DOUBLE,
  high          DOUBLE,
  low           DOUBLE,
  `close`       DOUBLE,
  volume        DOUBLE,
  trades        BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'candles',
  'properties.bootstrap.servers' = 'redpanda:9092',
  'format' = 'json'
);

INSERT INTO candles
SELECT
  symbol,
  window_start,
  window_end,
  FIRST_VALUE(price) AS `open`,
  MAX(price)         AS high,
  MIN(price)         AS low,
  LAST_VALUE(price)  AS `close`,
  SUM(qty)           AS volume,
  COUNT(*)           AS trades
FROM TABLE(
  TUMBLE(TABLE ticks, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
)
WHERE `source` <> 'test'
GROUP BY symbol, window_start, window_end;
