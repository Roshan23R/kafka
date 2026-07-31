"""
Pattern 4: Log Aggregation
----------------------------
Tails the combined "logs" topic -- every service's logs land here in
one place. Supports optional filters so you can simulate the "search
all-logs" box from the infographic without needing a real search
engine wired up.

Run:
    python log_aggregator.py
    python log_aggregator.py --level ERROR
    python log_aggregator.py --service svc-b
    python log_aggregator.py --level ERROR --service svc-a
"""

import json
import argparse
from confluent_kafka import Consumer

TOPIC = "logs"

# ANSI colors just to make ERROR/WARN visually pop in the terminal
COLORS = {"ERROR": "\033[91m", "WARN": "\033[93m", "INFO": "\033[92m"}
RESET = "\033[0m"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--level", help="Only show this log level, e.g. ERROR")
    parser.add_argument("--service", help="Only show logs from this service, e.g. svc-a")
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "log-aggregator",
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([TOPIC])

    filters = []
    if args.level:
        filters.append(f"level={args.level}")
    if args.service:
        filters.append(f"service={args.service}")
    filter_desc = f" (filter: {', '.join(filters)})" if filters else " (no filter -- showing all)"
    print(f"Tailing '{TOPIC}'{filter_desc}. Ctrl+C to stop.\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            log = json.loads(msg.value())

            if args.level and log["level"] != args.level:
                continue
            if args.service and log["service"] != args.service:
                continue

            color = COLORS.get(log["level"], "")
            print(f"{color}[p{msg.partition()}] [{log['service']}] {log['level']:<5} {log['message']}{RESET}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()