"""
Pattern 5: Stream Processing
------------------------------
Emits a continuous stream of e-commerce click events (user browsing
product pages). The stream processor will count views per category
inside tumbling time windows.

Run:
    python producer.py
"""

import time
import json
import random
from confluent_kafka import Producer

TOPIC = "clicks"

CATEGORIES = {
    "electronics": ["/laptop/99", "/phone/42", "/tablet/17", "/tv/88"],
    "clothing":    ["/shirt/12", "/jeans/55", "/shoes/31"],
    "books":       ["/novel/7",  "/sci-fi/23", "/history/4"],
    "gaming":      ["/console/60", "/game/77", "/headset/9"],
}

USER_IDS = [f"u{i}" for i in range(1, 21)]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    pages = [(cat, page) for cat, pages in CATEGORIES.items() for page in pages]

    print(f"Streaming click events to '{TOPIC}' (Ctrl+C to stop)...")
    try:
        while True:
            category, page = random.choice(pages)
            event = {
                "user_id":  random.choice(USER_IDS),
                "category": category,
                "page":     page,
                "ts":       time.time(),
            }
            producer.produce(
                TOPIC,
                key=event["user_id"],
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(random.uniform(0.05, 0.2))
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
