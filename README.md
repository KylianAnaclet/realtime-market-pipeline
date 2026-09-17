# realtime-market-pipeline

A real-time market data pipeline: live crypto trades from Binance, ingested
into a message bus and aggregated into per-minute OHLC candles.

**Status: work in progress — Palier 1 (hot path).** Ingestion works; the
stream processing and dashboard are not built yet.

## What runs today


- A Python producer holds a WebSocket open on Binance `@trade` streams and
  publishes every trade to the `ticks` topic.
- Messages are keyed by symbol, so all trades of a given symbol stay ordered
  within one partition.
- Each tick carries three timestamps: trade time (market), event time
  (exchange), and ingest time (this pipeline) — so the delay between them
  can be measured.

## Run it

```bash
docker compose up -d --build
docker compose logs -f producer
docker compose exec redpanda rpk topic consume ticks -o end -n 1
```

Redpanda Console: http://localhost:8080

## Next

- Measure the event-time / ingest-time distribution
- Per-minute OHLC candles in Apache Flink, windowed on event-time
- Live dashboard, and failure/burst experiments

## Design decisions

See [DECISIONS.md](DECISIONS.md) — every non-obvious choice, with what it
costs.
