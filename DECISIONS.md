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


### D-004 — Flink SQL instead of PyFlink
*2026-09-18*

**Choice:** Write the job as Flink SQL, run from the SQL client.
**Instead of:** PyFlink (the Python API).
**Why:** There is no Python package of Flink built for Linux on ARM, so
installing it in a container on my M1 would compile it from source. SQL
runs on the same engine with the same watermarks and the same state
handling, and the aggregation is 30 lines instead of 150.
**Trade-off accepted:** No custom state logic, which this aggregation
doesn't need. I'd use the DataStream API if it did.


### D-005 — 2-second watermark
*2026-09-18*

**Choice:** Close a window 2 seconds after the newest event time seen.
**Instead of:** Zero, which my measurements would have allowed.
**Why:** I measured zero out-of-order events over 2.3M messages, but that
zero comes from having a single producer on a single connection (see
MEASUREMENTS.md). It would break with a second producer or another
source. Waiting 2 seconds on a 60-second window costs a little freshness;
not waiting produces silently wrong candles. The costs aren't symmetric,
so I take the cheap safe side.
**Trade-off accepted:** Candles are published about 2 seconds later than
they could be.

### D-006 — Local execution mode (temporary)
*2026-09-18*

**Choice:** Run the SQL job inside the client process, in its own
container, with no Flink cluster.
**Instead of:** Submitting to the JobManager/TaskManager cluster, which
is what the repo is set up for.
**Why:** Submitting to the cluster fails on an upstream bug in the
multipart decoder Flink bundles: the upload is split so that a line
break falls between two chunks and the decoder returns null. It is not
configuration-dependent and changing Flink version didn't help.
**Trade-off accepted:** This is a workaround, not a deployment. In this
mode there is no fault tolerance and no exactly-once: stopping the
container loses the windows in flight. To be revisited when the pipeline
needs the cluster for checkpointing and backpressure metrics.
