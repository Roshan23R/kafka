"""
Pattern Bonus: Fraud Detection
------------------------------
Downstream consumer of the enriched stream. Only prints HIGH VALUE
transactions — simulating an alerting service or fraud review queue
that only cares about the processor's output, not the raw events.

Run (with producer.py and processor.py already running):
    python consumer.py
"""

import json
from confluent_kafka import Consumer

TOPIC = "transactions-enriched"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id":          "alert-consumer",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    print(f"Watching '{TOPIC}' for high-value transactions (Ctrl+C to stop)...\n")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            txn = json.loads(msg.value())

            if not txn["high_value"]:
                continue

            print(
                f"\033[91m[ALERT]\033[0m {txn['user_id']} spent ${txn['amount']:.2f} "
                f"at {txn['merchant']} "
                f"(session total: ${txn['running_total']:.2f})"
            )
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
