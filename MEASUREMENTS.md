# Measurements

Figures produced by the `stats` consumer, which reads the `ticks` topic
and reports every 10 seconds.

## What I measure

- **Delay** = `ingest_time_ms − event_time_ms`: time between a trade
  executing on the exchange and my producer receiving it. Both
  timestamps are in the message, so this can be recomputed on replay.
- **Out-of-orderness** = how far behind the newest event of its
  partition a tick arrives. This is what sizes a watermark, not delay.

I injected a tick dated 2001 by hand to check the counter actually
fires. It reported 1 out-of-order event with the right lateness, so the
zeros below are real measurements and not a dead counter.

## Run 1 — 10 pairs

*2026-09-17 evening CEST, live flow only, MacBook Air M1, 9 min,
22 700 ticks.*

throughput 8–154/s (avg ~40/s) · p50 55 ms · p95 292 ms · p99 428 ms ·
max 1 014 ms · **0 out-of-order**

## Run 2 — 40 pairs

*Same host and conditions, 5.5 min, 30 300 ticks.*

throughput 41–237/s (avg ~92/s) · p50 36 ms · p95 283 ms · p99 380 ms ·
max 743 ms · **0 out-of-order**

Load went up 2.3× and every percentile went down, so the producer is not
the bottleneck at these rates. I don't know why p50 improved — network
conditions and time of day differed — so I'm not claiming it as a fix.

## Why zero out-of-order

One WebSocket, one process, one write order. TCP keeps ordering inside a
connection and Binance sends a symbol's trades in execution order, so
nothing in the chain can reorder anything.

That makes the zero fragile: it would break with a second producer, a
second connection, or another data source. I size the watermark for
those cases, not for the zero I measured.

## A run I threw away

An earlier run gave p99 = 24 202 ms and max = 33 882 ms — 56× worse,
same code. It mixed hours of historical replay (host sleep,
reconnections, CPU starvation from the replay itself) with live flow.
Keeping it here because the gap between the two runs is bigger than
anything the system itself does.

## Known gaps

- Binance's clock and mine aren't synchronised, so part of the median
  delay may be clock offset.
- Ticks lost during a disconnection are gone — no backfill, and nothing
  detects the gap yet.
- The topic still contains my test record; an append-only log can't be
  edited, so it's filtered downstream.
