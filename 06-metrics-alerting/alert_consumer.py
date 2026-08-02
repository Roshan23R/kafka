"""
Pattern 6: Metrics & Alerting
--------------------------------
Watches only the cpu-alerts topic -- simulating an on-call notification
service (PagerDuty/Slack bot) that never touches the raw metrics
stream, just the ALERT/RESOLVED events the processor decided to fire.

Run:
    python alert_consumer.py
"""

import json
from datetime import datetime
from confluent_kafka import Consumer

TOPIC = "cpu-alerts"


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "oncall-notifier",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    print("Watching cpu-alerts (Ctrl+C to stop)...")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            alert = json.loads(msg.value())
            icon = "🔥" if alert["status"] == "ALERT" else "✅"
            print(f"{icon} [{datetime.fromtimestamp(alert['ts']).strftime('%Y-%m-%d %H:%M:%S')}] [{alert['source']}] {alert['status']} -- rolling avg {alert['rolling_avg']}%")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()