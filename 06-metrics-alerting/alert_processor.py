"""
Pattern 6: Metrics & Alerting
--------------------------------
Maintains a SLIDING window (as opposed to Pattern 5's TUMBLING window)
of the last 10 readings per source, and computes a rolling average on
every single new reading. When that rolling average crosses the alert
threshold, it fires ONE alert event -- not one alert per reading past
the threshold, thanks to an edge-trigger (state machine) that only
fires on the OK -> ALERT transition, and again on ALERT -> RESOLVED.

Sliding vs tumbling, concretely:
- Tumbling (Pattern 5): windows are fixed, non-overlapping. Window
  [0-5s] closes completely, then [5-10s] starts fresh with zero data.
- Sliding (this file): the window is always "the last 10 readings,"
  and it moves forward by one reading at a time -- readings overlap
  between one window-computation and the next. This is why metrics
  dashboards look smooth instead of jumping in steps.

Run:
    python alerting_processor.py
"""

import json
from collections import deque
from confluent_kafka import Consumer, Producer

INPUT_TOPIC = "cpu-metrics"
ALERT_TOPIC = "cpu-alerts"

WINDOW_SIZE = 10          # sliding window = last N readings
ALERT_THRESHOLD = 75.0    # rolling average CPU% that triggers an alert


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "alerting-processor",
            "auto.offset.reset": "latest",  # only care about metrics from now on, not old history
        }
    )
    consumer.subscribe([INPUT_TOPIC])
    producer = Producer({"bootstrap.servers": "localhost:9092"})

    # sliding window state per source: a deque with maxlen automatically
    # drops the oldest reading once it's full -- that's what makes it "slide"
    windows = {}
    # current alert state per source, so we only fire on OK->ALERT and
    # ALERT->OK transitions, never once per reading
    alert_state = {}

    print(f"Watching cpu-metrics, alerting when rolling avg >= {ALERT_THRESHOLD}% (Ctrl+C to stop)...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            reading = json.loads(msg.value())
            source = reading["source"]

            window = windows.setdefault(source, deque(maxlen=WINDOW_SIZE))
            window.append(reading["cpu_pct"])
            rolling_avg = round(sum(window) / len(window), 1)

            is_breaching = rolling_avg >= ALERT_THRESHOLD
            was_breaching = alert_state.get(source, False)

            print(f"  [{source}] cpu={reading['cpu_pct']}%  rolling_avg={rolling_avg}%  n={len(window)}")

            if is_breaching and not was_breaching:
                # OK -> ALERT edge: fire exactly once, not on every reading
                alert = {
                    "source": source,
                    "rolling_avg": rolling_avg,
                    "status": "ALERT",
                    "ts": reading["ts"],
                }
                producer.produce(ALERT_TOPIC, key=source, value=json.dumps(alert), callback=delivery_report)
                producer.poll(0)
                print(f"  !!! ALERT [{source}] rolling avg {rolling_avg}% >= {ALERT_THRESHOLD}%")
            elif was_breaching and not is_breaching:
                # ALERT -> OK edge: fire a resolved event
                resolved = {
                    "source": source,
                    "rolling_avg": rolling_avg,
                    "status": "RESOLVED",
                    "ts": reading["ts"],
                }
                producer.produce(ALERT_TOPIC, key=source, value=json.dumps(resolved), callback=delivery_report)
                producer.poll(0)
                print(f"  ✓ RESOLVED [{source}] rolling avg back to {rolling_avg}%")

            alert_state[source] = is_breaching
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()