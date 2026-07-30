"""
Pattern 2: Pub-Sub Fan-Out
---------------------------
A generic "subscriber" -- run this three times with three different
--role values (email, analytics, billing). Each run uses a DIFFERENT
group.id, so each becomes its own independent consumer group and
receives every single event, regardless of what the other roles do.
 
Run (in 3 separate terminals):
    python subscriber.py --role email
    python subscriber.py --role analytics
    python subscriber.py --role billing
"""

import argparse
import json
from confluent_kafka import Consumer, KafkaError

TOPIC = "order-placed"

# what each "service" pretends to do with the event
ACTIONS = {
    "email": lambda e: f"Sending confirmation email to {e['customer_email']}",
    "analytics": lambda e: f"Recording order {e['order_id']} for revenue dashboard (amount={e['amount']})",
    "billing": lambda e: f"Charging {e['amount']} for order {e['order_id']}",
}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, choices=ACTIONS.keys())
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"order-placed-{args.role}",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    group_id = f"order-placed-{args.role}"

    print(f"[{args.role}] subscribed as group '{group_id}', waiting for events...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[{args.role}] error: {msg.error()}")
                continue
 
            event = json.loads(msg.value())
            action = ACTIONS[args.role](event)
            print(f"[{args.role}] {action}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
 
 
if __name__ == "__main__":
    main()