"""
Pattern Bonus: Fraud Detection
------------------------------
Simulates a payment gateway emitting raw transactions continuously to
the `transactions` topic. Each transaction has a user_id, amount, and
merchant. The stream processor (processor.py) will read this topic,
enrich each event, and write results to a second topic.

Run:
    python producer.py
"""

import time
import json
import random
from confluent_kafka import Producer

TOPIC = "transactions"

MERCHANTS = ["amazon", "netflix", "uber", "starbucks", "apple", "steam"]
USER_IDS  = [f"user-{i}" for i in range(1, 11)]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print(f"Streaming transactions to '{TOPIC}' (Ctrl+C to stop)...")
    try:
        while True:
            txn = {
                "txn_id":   random.randint(10000, 99999),
                "user_id":  random.choice(USER_IDS),
                "merchant": random.choice(MERCHANTS),
                # most transactions are small; occasional large spike
                "amount":   round(random.choices(
                    [random.uniform(1, 50), random.uniform(200, 1000)],
                    weights=[85, 15]
                )[0], 2),
                "ts": time.time(),
            }
            producer.produce(
                TOPIC,
                key=txn["user_id"],
                value=json.dumps(txn),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(random.uniform(0.2, 0.6))
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
