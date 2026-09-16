### D-001 --  Redpanda as the message bus
*2026-09-15*

**Choice:** Redpanda, single node, in Docker.
**Instead of:** Apache Kafka.
**Why:** Easier to operate alone — one binary, no JVM, no Zookeeper, so a
single service in docker-compose instead of two. Lower resource footprint,
which matters because the same 8GB laptop also runs Flink, Grafana and a
browser. And it speaks the Kafka protocol, so nothing in my code is
Redpanda-specific.


### D-002 — Partition key by symbol
*2026-09-16*

**Choice:** Key each tick by its symbol.
**Instead of:** No key (round-robin placement).
**Why:** Ordering is guaranteed within a partition only. Keying by symbol
keeps all trades of a given symbol in one partition, so they stay in the
order the exchange sent them. The OHLC aggregation (not implemented yet)
needs that: open is the first trade of the minute and close is the last,
so both are defined by order.
**Trade-off accepted:** Uneven partition load — BTCUSDT is far busier than
the others. And no ordering guarantee between symbols, which this pipeline
never needs.

### D-003 — Trade time (T) as event-time
*2026-09-16*

**Choice:** Use Binance's trade time `T` as the event timestamp.
**Instead of:** Event time `E`.
**Why:** `T` describes the market — when the trade actually executed.
`E` describes Binance — when its server emitted the message, which
includes their internal processing delay. Candles claim to summarise the
market, so they must be bucketed on market time.
**Trade-off accepted:** `T` is the earliest of the three timestamps, so the
gap to ingest time is wider and the watermark will have to tolerate more
delay. I also trust the exchange's clock, which I don't control.


