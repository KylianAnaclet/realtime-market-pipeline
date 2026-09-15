### D-001 — Redpanda as the message bus
*2026-09-15*

**Choice:** Redpanda, single node, in Docker.
**Instead of:** Apache Kafka.
**Why:** Easier to operate alone — one binary, no JVM, no Zookeeper, so a
single service in docker-compose instead of two. Lower resource footprint,
which matters because the same 8GB laptop also runs Flink, Grafana and a
browser. And it speaks the Kafka protocol, so nothing in my code is
Redpanda-specific.
