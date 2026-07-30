# 1. Async Messaging

**Decouple producers from consumers.**

The producer fires 20 messages as fast as possible (no throttling). The
consumer processes each one slowly (0.5s simulated work). Because Kafka
sits between them, the producer finishes instantly and never blocks
waiting on the consumer — the topic (the log) absorbs the burst and the
consumer catches up on its own schedule.

## Run it

First make sure virtual environment is activated and dependencies are installed (see [Setup](../README.md#setup)).

1. Start the broker (from repo root):
   ```bash
   docker compose up -d
   ```
2. In one terminal:
   ```bash
   python consumer.py
   ```
3. In another terminal:
   ```bash
   python producer.py
   ```

## What to observe

- `producer.py` finishes in under a second even though it sent 20 messages.
- `consumer.py` is still working through the backlog 10 seconds later.
- Kill the consumer mid-way and restart it — it resumes from where it
  left off (consumer group offset), no messages lost.
