import asyncio
import json
import os
import time

import websockets
from confluent_kafka import Producer

BOOTSTRAP = os.getenv("BOOTSTRAP_SERVERS", "redpanda:9092")
TOPIC = os.getenv("TOPIC", "ticks")
SYMBOLS = [s.strip().lower() for s in os.getenv("SYMBOLS", "btcusdt").split(",")]

STREAM_URL = (
    "wss://stream.binance.com:9443/stream?streams="
    + "/".join(f"{s}@trade" for s in SYMBOLS)
)


def to_tick(data: dict) -> dict:
    """Convert one Binance trade message into our own tick record.
    """
    symbol = data['s'] # str
    trade_id = data['t'] # int
    price = data['p'] # Binance sends the price in string
    qty = data['q'] # same as price
    event_time_ms = data['T'] # int
    ingest_time_ms = int(time.time() * 1000)
    is_buyer_maker = data['m'] # bool
    
    res = {'symbol': symbol, 'trade_id': trade_id, 'price': price, 'qty': qty, 'event_time_ms': event_time_ms, 'ingest_time_ms': ingest_time_ms, 'is_buyer_maker': is_buyer_maker, 'source': 'binance'}

    return res

def on_delivery(err, msg):
    if err is not None:
        print(f"[producer] delivery failed: {err}")


async def run():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "linger.ms": 50,
        "compression.type": "lz4",
        "enable.idempotence": True,
    })

    backoff = 1
    count, window_start = 0, time.time()

    while True:
        try:
            async with websockets.connect(STREAM_URL, ping_interval=20) as ws:
                print(f"[producer] connected to {len(SYMBOLS)} streams")
                backoff = 1
                
                bad = 0
                async for raw in ws:
                    try:
                        envelope = json.loads(raw)
                        data = envelope.get("data", envelope)
                        if data.get("e") != "trade":
                            continue
                        tick = to_tick(data)
                    except Exception as e:
                        bad += 1
                        if bad <= 3:
                            print(f"[producer] bad message ({type(e).__name__}: {e})")
                            print(f"[producer] raw: {raw[:200]}")
                        continue

                    producer.produce(
                        TOPIC,
                        key=tick["symbol"].encode(),
                        value=json.dumps(tick).encode(),
                        on_delivery=on_delivery,
                    )
                    producer.poll(0)

                    count += 1
                    elapsed = time.time() - window_start
                    if elapsed >= 5:
                        print(f"[producer] {count / elapsed:.0f} ticks/s")
                        count, window_start = 0, time.time()

        except Exception as e:
            print(f"[producer] connection lost ({e}) — retry in {backoff}s")
            producer.flush(5)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)


asyncio.run(run())
