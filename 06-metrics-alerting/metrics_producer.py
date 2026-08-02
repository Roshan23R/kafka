"""
Pattern 6: Metrics & Alerting
--------------------------------
Emits CPU usage readings for two services ("api", "db") continuously.
Most of the time values sit in a normal range, but every so often a
service enters a "spike" period (simulating a real incident) where
values run high for several seconds in a row -- enough to actually
trigger the alerting logic in alerting_processor.py, not just brush
past the threshold for a single reading.

Run:
    python metrics_producer.py
"""

import time
import json
import random
from confluent_kafka import Producer

TOPIC = "cpu-metrics"
SOURCES = ["api", "db"]

# each source can be in a "spike" state -- once triggered, values stay
# high for a few seconds, then recover, so the sliding average actually
# has time to cross the alert threshold instead of one noisy blip
spike_until = {s: 0 for s in SOURCES}
# {"api": 0, "db": 0}


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def next_value(source):
    now = time.time()
    if now < spike_until[source]:
        return round(random.uniform(85, 99), 1)

    # small random chance to enter a spike period
    if random.random() < 0.03:
        spike_until[source] = now + random.uniform(4, 8)
        return round(random.uniform(85, 99), 1)

    return round(random.uniform(15, 55), 1)


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print("Streaming CPU metrics for 'api' and 'db' (Ctrl+C to stop)...")
    try:
        while True:
            for source in SOURCES:
                reading = {
                    "source": source,
                    "cpu_pct": next_value(source),
                    "ts": time.time(),
                }
                producer.produce(
                    TOPIC,
                    key=source,
                    value=json.dumps(reading),
                    callback=delivery_report,
                )
                producer.poll(0)
                print(f"  {source}: {reading['cpu_pct']}%")
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()