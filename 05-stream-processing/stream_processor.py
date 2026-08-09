"""
Pattern 5: Stream Processing
------------------------------
Tumbling window processor: reads the `clicks` topic, buckets each event
into a fixed-size time window (default 10 seconds), and when a window
closes it emits a summary record to `click-windows` showing the view
count per category for that window.

This is the defining operation of stream processing -- you are not just
forwarding or filtering events one-by-one; you are accumulating state
across a time boundary and emitting derived aggregates.

Run (keep producer.py running in another terminal):
    python processor.py
    python processor.py --window 5    # 5-second windows
"""

import time
import json
import argparse
from collections import defaultdict
from confluent_kafka import Consumer, Producer

INPUT_TOPIC  = "clicks"
OUTPUT_TOPIC = "click-windows"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def emit_window(producer, window_start, window_end, counts):
    summary = {
        "window_start": window_start,
        "window_end":   window_end,
        "counts":       dict(counts),
        "total":        sum(counts.values()),
    }
    producer.produce(
        OUTPUT_TOPIC,
        key=f"{window_start:.0f}",
        value=json.dumps(summary),
        callback=delivery_report,
    )
    producer.poll(0)

    # just display the window summary in the console for this demo;
    # print(f"\n── Window {time.strftime('%H:%M:%S', time.localtime(window_start))} "
    #       f"→ {time.strftime('%H:%M:%S', time.localtime(window_end))} ──")
    # for cat, count in sorted(counts.items(), key=lambda x: -x[1]):
    #     bar = "█" * count
    #     print(f"  {cat:<15} {bar} ({count})")
    # print(f"  {'TOTAL':<15} {summary['total']}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--window", type=int, default=10,
                        help="Tumbling window size in seconds (default: 10)")
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id":          "stream-processor",
            "auto.offset.reset": "earliest",
        }
    )
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    consumer.subscribe([INPUT_TOPIC])

    window_start  = time.time()
    window_counts = defaultdict(int)   # category → count within current window

    print(f"Processing '{INPUT_TOPIC}' with {args.window}s tumbling windows "
          f"→ '{OUTPUT_TOPIC}' (Ctrl+C to stop)...")
    try:
        while True:
            now = time.time()

            # close the window when its duration has elapsed
            if now - window_start >= args.window:
                emit_window(producer, window_start, now, window_counts)
                print(f"\n── Window {time.strftime('%H:%M:%S', time.localtime(window_start))} "
                        f"→ {time.strftime('%H:%M:%S', time.localtime(now))} ──")
                print(f"  {'TOTAL':<15} {sum(window_counts.values())}")
                window_start  = now
                window_counts = defaultdict(int)

            msg = consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value())
            window_counts[event["category"]] += 1


    except KeyboardInterrupt:
        # emit the partial window on shutdown
        if window_counts:
            emit_window(producer, window_start, time.time(), window_counts)
        print("\nStopping processor...")
    finally:
        consumer.close()
        producer.flush()


if __name__ == "__main__":
    main()
