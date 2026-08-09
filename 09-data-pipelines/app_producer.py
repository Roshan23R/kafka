"""
Pattern 9: Data Pipelines
--------------------------
Simulates an application source: continuously produces user-action events
into the 'app-events' topic, as if a web/mobile app were streaming
behavioural data into Kafka alongside the database source.

Run:
    python app_producer.py
"""

import time
import json
import random
from confluent_kafka import Producer

TOPIC = "app-events"

ACTIONS  = ["signup", "login", "view_product", "add_to_cart", "checkout", "logout"]
PAGES    = ["/home", "/products", "/cart", "/checkout", "/account", "/search"]
USER_IDS = [f"user-{i}" for i in range(1, 101)]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    event_id = 1
    print(f"Streaming app events → '{TOPIC}' (Ctrl+C to stop)...")
    try:
        while True:
            event = {
                "event_id": event_id,
                "user_id":  random.choice(USER_IDS),
                "action":   random.choice(ACTIONS),
                "page":     random.choice(PAGES),
                "ts":       time.time(),
            }
            producer.produce(
                TOPIC,
                key=event["user_id"],
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)
            event_id += 1
            time.sleep(random.uniform(0.02, 0.1))
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"\nDone. Produced {event_id - 1} app events.")


if __name__ == "__main__":
    main()
