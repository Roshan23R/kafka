"""
Pattern 1: Async Messaging
---------------------------
Producer fires messages fast (simulating a burst of ~20/sec) and never
waits for the consumer. Kafka's topic buffers the burst; the consumer
drains it at its own pace. This is the core decoupling Kafka gives you.

Run:
    python producer.py
"""

import time
import json
from confluent_kafka import Producer

TOPIC = "orders"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Produced -> partition {msg.partition()} offset {msg.offset()}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print("Producing 20 messages as fast as possible (simulated burst)...")
    for i in range(20):
        payload = {
            "order_id": i,
            "amount": round(10 + i * 1.5, 2),
            "ts": time.time(),
        }
        producer.produce(
            TOPIC,
            key=str(i),
            value=json.dumps(payload),
            callback=delivery_report,
        )
        # poll(0) triggers delivery callbacks without blocking the send
        producer.poll(0)
        # no sleep here on purpose -- we want to simulate a burst

    producer.flush()
    print("Done producing. Notice the consumer (run separately) can lag behind.")


if __name__ == "__main__":
    main()
