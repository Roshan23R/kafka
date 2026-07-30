"""
Pattern 1: Async Messaging
---------------------------
Consumer deliberately processes slowly (simulating real work like a DB
write or an API call) to demonstrate that the producer was never blocked
waiting for it -- the topic absorbed the burst.

Run (in a separate terminal from producer.py):
    python consumer.py
"""

import time
import json
from confluent_kafka import Consumer

TOPIC = "orders"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "async-messaging-consumer",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    print("Consuming slowly (simulating ~0.5s of work per message)...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            order = json.loads(msg.value())
            print(f"Consumed order {order['order_id']} (amount={order['amount']})")

            # simulate slow downstream work (DB write, email send, etc.)
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
