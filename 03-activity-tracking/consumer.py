"""
Pattern 3: Activity Tracking
-----------------------------
Consumes the click stream and maintains a LIVE in-memory count --
total events, and a breakdown per page -- redrawing the terminal every
time a new batch of events comes in. This is the simplest possible
version of "real-time dashboard" (panel 3 in the infographic).

Run:
    python consumer.py
"""

import json
from collections import Counter
from confluent_kafka import Consumer

TOPIC = "user-clicks"


def render(total, per_page, per_type):
    # \033[2J\033[H clears the terminal and moves cursor to top,
    # so the counts update in place instead of scrolling forever
    print("\033[2J\033[H", end="")
    print("LIVE ACTIVITY DASHBOARD")
    print("=" * 40)
    print(f"Total events: {total}")
    print("\nBy page:")
    for page, count in per_page.most_common():
        print(f"  {page:<15} {count}")
    print("\nBy event type:")
    for etype, count in per_type.most_common():
        print(f"  {etype:<15} {count}")
    print("\n(Ctrl+C to stop)")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "activity-tracking-consumer",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    total = 0
    per_page = Counter()
    per_type = Counter()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value())
            total += 1
            per_page[event["page"]] += 1
            per_type[event["event_type"]] += 1

            render(total, per_page, per_type)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()