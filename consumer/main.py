import json
import os
import time
from collections import defaultdict, deque

from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.getenv("TOPIC", "ticks")
GROUP = os.getenv("GROUP_ID", "tick-stats")
REPORT_EVERY = int(os.getenv("REPORT_EVERY", "10"))
START_AT = os.getenv("START_AT", "earliest")
SAMPLE = 200_000

# partition -> highest event_time seen so far on that partition
max_event_time = defaultdict(int)


def observe(partition: int, event_time_ms: int) -> int:
    """How far behind the newest event of this partition is this one?
    """
    if event_time_ms >= max_event_time[partition]:
        # right order
        max_event_time[partition] = event_time_ms
        return 0
    
    late = max_event_time[partition] - event_time_ms
    return late


def pct(values, p):
    if not values:
        return 0
    s = sorted(values)
    return s[min(len(s) - 1, int(len(s) * p / 100))]

def parse(raw):
    try:
        tick = json.loads(raw)
    except ValueError:
        return None
    if tick.get("source") == "test":
        return None
    if "event_time_ms" in tick and "ingest_time_ms" in tick:
        return tick
    return None

def report(n, window_n, window_s, out_of_order, skipped,
           delays, lateness, max_delay_ever):
    print(
        f"\n[stats] {n} total | {window_n / window_s:.0f}/s over last {window_s:.0f}s\n"
        f"  delay    p50={pct(delays, 50)}ms  p95={pct(delays, 95)}ms  "
        f"p99={pct(delays, 99)}ms  max={max_delay_ever}ms\n"
        f"  lateness p99={pct(lateness, 99)}ms  max={max(lateness, default=0)}ms\n"
        f"  out of order: {out_of_order}  |  skipped: {skipped}",
        flush=True,
    )

def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": START_AT,
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])

    delays = deque(maxlen=SAMPLE)
    lateness = deque(maxlen=SAMPLE)
    n = out_of_order = 0
    skipped = 0
    start = last_report = time.time()
    max_delay_ever = 0
    n_at_last_report = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                tick = parse(msg.value())
                if tick is None:
                    skipped += 1
                else:
                    delay = tick["ingest_time_ms"] - tick["event_time_ms"] 
                    delays.append(delay)
                    max_delay_ever = max(max_delay_ever, delay)
                    late = observe(msg.partition(), tick["event_time_ms"])
                    lateness.append(late)
                    if late > 0:
                        out_of_order += 1
                    n += 1

            elif msg is not None:
                print(f"[stats] error: {msg.error()}")
            now = time.time()
            if now - last_report >= REPORT_EVERY:
                report(n, n - n_at_last_report, now - last_report,
                       out_of_order, skipped, delays, lateness, max_delay_ever)
                n_at_last_report = n
                last_report = now
    except KeyboardInterrupt:
        report(n, out_of_order, delays, lateness, time.time() - start)
    finally:
        consumer.close()


main()
