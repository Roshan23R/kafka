"""
Pattern 3: Activity Tracking
-----------------------------
Simulates a website streaming user click events continuously (like a
real frontend would via a /track endpoint -> Kafka). Runs forever,
producing one event every ~0.2s until you stop it with Ctrl+C.

Run:
    python producer.py
"""

import time
import json
import random
from confluent_kafka import Producer

TOPIC = "user-clicks"

PAGES = ["/home", "/product/42", "/cart", "/checkout", "/search", "/profile"]
EVENT_TYPES = ["page_view", "click", "add_to_cart", "scroll"]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print("Streaming click events (Ctrl+C to stop)...")
    try:
        while True:
            event = {
                "user_id": random.randint(1, 500),
                "page": random.choice(PAGES),
                "event_type": random.choice(EVENT_TYPES),
                "ts": time.time(),
            }
            producer.produce(
                TOPIC,
                key=str(event["user_id"]),
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(0.2)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()