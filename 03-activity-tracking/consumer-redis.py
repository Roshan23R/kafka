"""
Pattern 3: Activity Tracking (with Redis persistence)
-------------------------------------------------------
Same live dashboard as before, but now the aggregate counts (total,
per-page, per-type) are stored in Redis instead of plain Python
variables. On startup, we LOAD existing counts from Redis first, so
restarting this script resumes the dashboard instead of resetting it
to zero.

This directly fixes the gap called out in the Pattern 3 README: Kafka
remembers which messages were read (offsets), but never remembers any
aggregate YOU computed from them -- that's on you to persist somewhere
durable. Redis is that "somewhere" here.

Run:
    python consumer.py
"""

import json
from collections import Counter
from confluent_kafka import Consumer
import redis

TOPIC = "user-clicks"

TOTAL_KEY = "activity:total"
PER_PAGE_KEY = "activity:per_page"   # Redis hash: page -> count
PER_TYPE_KEY = "activity:per_type"   # Redis hash: event_type -> count


def render(total, per_page, per_type):
    print("\033[2J\033[H", end="")
    print("LIVE ACTIVITY DASHBOARD (backed by Redis)")
    print("=" * 40)
    print(f"Total events: {total}")
    print("\nBy page:")
    for page, count in per_page.most_common():
        print(f"  {page:<15} {count}")
    print("\nBy event type:")
    for etype, count in per_type.most_common():
        print(f"  {etype:<15} {count}")
    print("\n(Ctrl+C to stop -- restart and counts will resume, not reset)")


def load_existing_counts(r):
    """Read whatever was already persisted from a previous run."""
    total = int(r.get(TOTAL_KEY) or 0)
    per_page = Counter({k: int(v) for k, v in r.hgetall(PER_PAGE_KEY).items()})
    per_type = Counter({k: int(v) for k, v in r.hgetall(PER_TYPE_KEY).items()})
    return total, per_page, per_type


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "activity-tracking-consumer-redis",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    # decode_responses=True -> get plain str back instead of bytes
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    total, per_page, per_type = load_existing_counts(r)
    print(f"Loaded {total} existing events from Redis. Waiting for new events...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value())

            # update Redis first (the durable copy)...
            r.incr(TOTAL_KEY)
            r.hincrby(PER_PAGE_KEY, event["page"], 1)
            r.hincrby(PER_TYPE_KEY, event["event_type"], 1)

            # ...then update the local in-memory copy used for rendering
            total += 1
            per_page[event["page"]] += 1
            per_type[event["event_type"]] += 1

            render(total, per_page, per_type)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()