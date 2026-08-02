"""
Pattern 7: Event Sourcing
----------------------------
Emits DEPOSIT / WITHDRAW events for a few accounts. Notice what's
NOT here: there is no "current balance" field anywhere, no database
row being updated. Each event only describes a CHANGE ("deposit $50"),
never the resulting total. The account-events topic itself -- the
append-only sequence of every change ever made -- IS the source of
truth. Current balance is a derived value, computed by replaying the
log (see rebuild_balance.py), not something stored directly.

Run:
    python events_producer.py
"""

import time
import json
import random
from confluent_kafka import Producer

TOPIC = "account-events"
ACCOUNTS = ["acc-1", "acc-2"]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def random_event():
    account_id = random.choice(ACCOUNTS)
    event_type = random.choices(["DEPOSIT", "WITHDRAW"], weights=[0.6, 0.4])[0]
    amount = round(random.uniform(10, 200), 2)
    return {
        "account_id": account_id,
        "event_type": event_type,
        "amount": amount,
        "ts": time.time(),
    }


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print("Streaming account events (Ctrl+C to stop)...")
    try:
        while True:
            event = random_event()
            # key by account_id -> all of one account's events land in the
            # same partition, so replaying preserves their true order
            producer.produce(
                TOPIC,
                key=event["account_id"],
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)
            sign = "+" if event["event_type"] == "DEPOSIT" else "-"
            print(f"  {event['account_id']}: {event['event_type']} {sign}{event['amount']}")
            time.sleep(random.uniform(0.5, 1.5))
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()