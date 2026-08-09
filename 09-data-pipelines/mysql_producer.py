"""
Pattern 9: Data Pipelines
--------------------------
Simulates a MySQL source: continuously produces order rows into the
'mysql-orders' topic, as if a CDC connector or ETL job were forwarding
database rows into Kafka.

Run:
    python mysql_producer.py
"""

import time
import json
import random
from confluent_kafka import Producer

TOPIC = "mysql-orders"

PRODUCTS = [
    "laptop", "phone", "tablet", "headphones",
    "monitor", "keyboard", "mouse", "charger",
]
CUSTOMER_IDS = [f"cust-{i}" for i in range(1, 51)]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    order_id = 1
    print(f"Streaming MySQL order rows → '{TOPIC}' (Ctrl+C to stop)...")
    try:
        while True:
            record = {
                "order_id":    order_id,
                "customer_id": random.choice(CUSTOMER_IDS),
                "product":     random.choice(PRODUCTS),
                "amount":      round(random.uniform(9.99, 999.99), 2),
                "created_at":  time.time(),
            }
            producer.produce(
                TOPIC,
                key=record["customer_id"],
                value=json.dumps(record),
                callback=delivery_report,
            )
            producer.poll(0)
            order_id += 1
            time.sleep(random.uniform(0.05, 0.15))
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        print(f"\nDone. Produced {order_id - 1} order records.")


if __name__ == "__main__":
    main()
