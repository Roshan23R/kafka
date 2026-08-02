"""
Pattern 8: Change Data Capture
----------------------------------
Reads Debezium's change events from the cdc.public.orders topic and
syncs them into Redis (simulating a cache) and prints a "search index"
line (standing in for a real search engine like Elasticsearch, kept
out of this repo to avoid another heavy service). This consumer NEVER
talks to Postgres directly -- everything it knows comes from the
Kafka topic, which Debezium populated by tailing Postgres's write-
ahead log (WAL).

Each Debezium message has an "op" field:
    c = create (INSERT)
    u = update (UPDATE)
    d = delete (DELETE)
    r = read    (initial snapshot, sent once when the connector first starts)

Run:
    python cdc_consumer.py
"""

import json
from confluent_kafka import Consumer
import redis

TOPIC = "cdc.public.orders"

OP_LABELS = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "SNAPSHOT"}


def unwrap(value_bytes):
    """Debezium's raw message is either {"schema":..., "payload": {...}}
    or, with schemas disabled (as configured in docker-compose.yml), just
    the payload dict directly. Handle both so this script works either way."""
    parsed = json.loads(value_bytes)
    return parsed.get("payload", parsed) if isinstance(parsed, dict) else parsed


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "cdc-sync-service",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    r = redis.Redis(host="localhost", port=6379, decode_responses=True)

    print(f"Watching {TOPIC} for changes (Ctrl+C to stop)...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            if msg.value() is None:
                # a "tombstone" -- Kafka Connect emits one of these right
                # after a delete event, with a null value, for its own
                # internal log-compaction bookkeeping. Not a real change.
                continue

            change = unwrap(msg.value())
            op = change.get("op")
            label = OP_LABELS.get(op, op)

            if op in ("c", "u", "r"):
                row = change["after"]
                order_id = row["order_id"]
                r.set(f"order:{order_id}", json.dumps(row))
                print(f"[{label}] order {order_id} -> cache updated: {row}")
                print(f"          -> search index updated (simulated)")

            elif op == "d":
                row = change["before"]
                order_id = row["order_id"]
                r.delete(f"order:{order_id}")
                print(f"[{label}] order {order_id} -> removed from cache")
                print(f"          -> removed from search index (simulated)")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
