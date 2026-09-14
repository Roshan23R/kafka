"""
Pattern 10: Replay & Recovery
--------------------------------
Emits simple numbered events continuously. Nothing fancy on the
producer side -- the interesting behavior in this pattern all happens
on the consumer side (resilient_consumer.py, replay_tool.py). This
producer just needs to keep a steady, visible stream going so you can
watch a consumer fall behind, "crash," and catch back up.

Run:
    python events_producer.py
"""

import time
import json
from confluent_kafka import Producer

TOPIC = "recovery-events"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main():
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print("Streaming numbered events (Ctrl+C to stop)...")
    event_id = 0
    try:
        while True:
            event = {"event_id": event_id, "payload": f"event-{event_id}", "ts": time.time()}
            producer.produce(
                TOPIC,
                key=str(event_id),
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)
            print(f"  produced event {event_id}")
            event_id += 1
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
