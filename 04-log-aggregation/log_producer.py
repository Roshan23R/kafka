"""
Pattern 4: Log Aggregation
----------------------------
Simulates ONE service continuously emitting log lines to a shared
"logs" topic. Run this 3 times with different --service values (svc-a,
svc-b, svc-c) to simulate multiple services all logging into the same
central stream -- exactly like shipping logs from many containers/hosts
into one place instead of grepping scattered log files.

Run (in 3 separate terminals):
    python log_producer.py --service svc-a
    python log_producer.py --service svc-b
    python log_producer.py --service svc-c
"""

import time
import json
import random
import argparse
from confluent_kafka import Producer

TOPIC = "logs"

LEVELS = ["INFO", "INFO", "INFO", "WARN", "ERROR"]  # weighted so ERROR is rarer
ENDPOINTS = ["/pay", "/user", "/cart", "/checkout", "/health"]
METHODS = ["GET", "POST"]


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"  -> [{msg.key().decode()}] delivered to partition {msg.partition()}")


def random_log_line(service):
    level = random.choice(LEVELS)
    method = random.choice(METHODS)
    endpoint = random.choice(ENDPOINTS)
    status = 500 if level == "ERROR" else random.choice([200, 200, 200, 201, 404])
    return {
        "service": service,
        "level": level,
        "message": f"{method} {endpoint} {status}",
        "ts": time.time(),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--service", required=True, help="Service name, e.g. svc-a")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": "localhost:9092"})

    print(f"[{args.service}] streaming logs to topic '{TOPIC}' (Ctrl+C to stop)...")
    try:
        while True:
            log_line = random_log_line(args.service)
            producer.produce(
                TOPIC,
                key=args.service,
                value=json.dumps(log_line),
                callback=delivery_report,
            )
            producer.poll(0)
            time.sleep(random.uniform(0.3, 1.0))
    except KeyboardInterrupt:
        print(f"\n[{args.service}] stopping...")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()