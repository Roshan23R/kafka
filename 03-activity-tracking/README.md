# 3. Activity Tracking

**Stream user clicks in real time.**

The producer simulates a website continuously streaming click/page-view
events (like a frontend hitting a `/track` endpoint that forwards to
Kafka). The consumer maintains a live in-memory tally and redraws a
simple terminal dashboard on every new event — the smallest possible
version of a real-time analytics view.

There are two consumer versions in this folder, to make a specific
point about state:

- **`consumer.py`** — counts live only in local Python variables.
  Restart the script, counts reset to zero, even though Kafka itself
  didn't lose anything.
- **`consumer_redis.py`** — counts are persisted to Redis on every
  event. On startup it *loads* existing counts from Redis first, so
  restarting resumes the dashboard instead of resetting it.

## Run it

1. Broker + Redis running (`docker compose up -d` from repo root).
2. Terminal 1 — pick one:
   ```bash
   python consumer.py          # resets to zero on restart
   # or
   python consumer_redis.py    # resumes from Redis on restart
   ```
3. Terminal 2:
   ```bash
   python producer.py
   ```

## What to observe

- The dashboard updates continuously as events arrive — total count,
  breakdown by page, breakdown by event type.
- Stop the producer (Ctrl+C), the dashboard just stops changing — no
  errors, the consumer is simply waiting for more messages.
- **With `consumer.py`**: stop and restart it. Kafka correctly
  remembers not to redeliver already-read messages (same `group.id`),
  but the dashboard still shows counts starting from zero — because
  the *aggregate* only ever lived in RAM, not the raw messages.
- **With `consumer_redis.py`**: stop and restart it. It prints
  `Loaded N existing events from Redis` and the dashboard picks up
  exactly where it left off.
- Check what's actually in Redis:
  ```bash
  docker exec -it redis redis-cli
  > GET activity:total
  > HGETALL activity:per_page
  > HGETALL activity:per_type
  ```
- **Known limitation, worth understanding rather than hiding**: this
  Redis write happens *before* the Kafka offset is committed. If the
  process crashes between the Redis write and the offset commit, a
  restart will re-read that same message and double-count it in Redis.
  This is Kafka's default **at-least-once** delivery — a message can
  be processed more than once if a consumer crashes at the wrong
  moment. Avoiding this entirely needs either idempotent writes (e.g.
  storing "last processed offset" alongside the count, in the same
  Redis transaction, and skipping if already applied) or Kafka's
  transactional/exactly-once producer-consumer APIs — real production
  concerns, and exactly what Kafka Streams' state stores are designed
  to handle correctly out of the box (Pattern 5).