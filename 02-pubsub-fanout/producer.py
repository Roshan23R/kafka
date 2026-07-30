"""
Pattern 2: Pub-Sub Fan-Out
---------------------------
One producer publishes "order.placed" events to a single topic.
Three independent consumer groups (Email, Analytics, Billing) each
subscribe to the SAME topic and each receive the FULL stream --
because each is a different group.id, Kafka doesn't split the
messages between them, it duplicates the stream per group.
 
Run:
    python producer.py
"""

import time
import json
from confluent_kafka import Producer

TOPIC = "order-placed"

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for message {msg.key()}: {err}")
    else:
        print(f"Message {msg.key()} delivered to {msg.topic()} [{msg.partition()}]")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print("Publishing 5 order.placed events...")
    for i in range(5):
        event = {
            "order_id": i,
            "customer_email": f"customer{i}@example.com",
            "amount": round(20 + i * 5.25, 2),
            "ts": time.time(),
        }
        producer.produce(
            TOPIC,
            key=str(i),
            value=json.dumps(event),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(0.3)
 
    producer.flush()
    print("Done. Each of Email / Analytics / Billing consumers received all 5 events independently.")

if __name__ == "__main__":
    main()