# Kafka Use Cases

Ten small, runnable demos of common Apache Kafka patterns, built with
Python (`confluent-kafka`) against a single local broker.

| # | Pattern | Folder |
|---|---------|--------|
| 1 | Async Messaging | [`01-async-messaging`](01-async-messaging) |
| 2 | Pub-Sub Fan-Out | `02-pubsub-fanout` |
| 3 | Activity Tracking | `03-activity-tracking` |
| 4 | Log Aggregation | `04-log-aggregation` |
| 5 | Stream Processing | `05-stream-processing` |
| 6 | Metrics & Alerting | `06-metrics-alerting` |
| 7 | Event Sourcing | `07-event-sourcing` |
| 8 | Change Data Capture | `08-cdc` |
| 9 | Data Pipelines | `09-data-pipelines` |
| 10 | Replay & Recovery | `10-replay-recovery` |

## Setup

```bash
python -m venv venv (Recommended Version: 3.10)
source venv/bin/activate (Linux/Mac) or `venv\Scripts\activate` (Windows)
pip install -r requirements.txt
docker compose up -d

Note: If multiple Python versions are installed, use `py -3.10 -m venv venv` to create the virtual environment
```

This starts:
- a single-node Kafka broker (KRaft mode, no Zookeeper) on `localhost:9092`
- Kafka UI at `http://localhost:8080` to browse topics/messages/offsets

Each numbered folder is self-contained — see its own README for what the
pattern demonstrates and how to run it.
