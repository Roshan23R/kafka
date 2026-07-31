"""
Pattern 5: Stream Processing
------------------------------
Reads the `click-windows` topic -- each message is a closed window
summary produced by processor.py. Prints each window's category
breakdown as it arrives, simulating a real-time analytics dashboard
fed by the processed stream rather than the raw event stream.

Run (with producer.py and processor.py already running):
    python consumer.py
"""

import json
import time
from confluent_kafka import Consumer

TOPIC = "click-windows"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id":          "window-dashboard",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    print(f"Watching '{TOPIC}' for window summaries (Ctrl+C to stop)...\n")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            w = json.loads(msg.value())
            start = time.strftime("%H:%M:%S", time.localtime(w["window_start"]))
            end   = time.strftime("%H:%M:%S", time.localtime(w["window_end"]))

            print(f"\033[36m[Window {start} → {end}]\033[0m  total={w['total']}")
            for cat, count in sorted(w["counts"].items(), key=lambda x: -x[1]):
                print(f"  {cat:<15} {count}")

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
