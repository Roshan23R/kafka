"""
Pattern Bonus: Fraud Detection
------------------------------
The stream processor: reads raw transactions from `transactions`,
enriches each event with a running per-user spend total, flags any
single transaction over $200 as high-value, then writes the enriched
event to `transactions-enriched`.

This is the consume → transform → produce pipeline that is the heart
of stream processing. In production this would be Kafka Streams, Flink,
or Spark Structured Streaming; here it is plain Python so the pattern
is visible without framework overhead.

Run (keep producer.py running in another terminal):
    python processor.py
"""

import json
from collections import defaultdict
from confluent_kafka import Consumer, Producer

INPUT_TOPIC  = "transactions"
OUTPUT_TOPIC = "transactions-enriched"

HIGH_VALUE_THRESHOLD = 200.0


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id":          "stream-processor",
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    consumer.subscribe([INPUT_TOPIC])

    # in-memory running total per user (resets if processor restarts)
    user_spend = defaultdict(float)

    print(f"Processing '{INPUT_TOPIC}' → '{OUTPUT_TOPIC}' (Ctrl+C to stop)...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            txn = json.loads(msg.value())

            # --- transform ---
            user_spend[txn["user_id"]] += txn["amount"]
            enriched = {
                **txn,
                "running_total": round(user_spend[txn["user_id"]], 2),
                "high_value":    txn["amount"] >= HIGH_VALUE_THRESHOLD,
            }

            tag = " *** HIGH VALUE ***" if enriched["high_value"] else ""
            print(
                f"  {txn['user_id']} | ${txn['amount']:>7.2f} "
                f"| total=${enriched['running_total']:>8.2f}{tag}"
            )

            producer.produce(
                OUTPUT_TOPIC,
                key=txn["user_id"],
                value=json.dumps(enriched),
                callback=delivery_report,
            )
            producer.poll(0)

    except KeyboardInterrupt:
        print("\nStopping processor...")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
