# 4. Log Aggregation

**All service logs in one stream.**

Three independent services (`svc-a`, `svc-b`, `svc-c`) each emit their
own log lines continuously. Instead of writing to separate log files
scattered across machines, they all publish to one shared `logs`
topic. A single aggregator then tails the combined stream — and can
filter it, similar to a simple "search all logs" box.

---

## Part 1 — Basic run (1 partition, default)

When the `logs` topic is auto-created by Kafka, it gets **1 partition**.
All three services' messages land in that single partition, interleaved
in arrival order.

1. Broker running (`docker compose up -d` from repo root).
2. Three terminals, one service each:
   ```bash
   python log_producer.py --service svc-a
   python log_producer.py --service svc-b
   python log_producer.py --service svc-c
   ```
3. A fourth terminal, tailing everything:
   ```bash
   python log_aggregator.py
   ```
   Or filtered:
   ```bash
   python log_aggregator.py --level ERROR
   python log_aggregator.py --service svc-b
   python log_aggregator.py --level ERROR --service svc-a
   ```

### What to observe

- All three services' logs appear in one interleaved stream.
- Every message shows `[p0]` — there is only one partition, so
  every message from every service lands on partition 0.
- Key-based routing exists but has no visible effect with 1 partition.

---

## Part 2 — 3 partitions (one per service)

Recreate the topic with **3 partitions** so Kafka can actually route
each service's messages to its own partition. Each service key
(`svc-a`, `svc-b`, `svc-c`) hashes to a different partition.

1. Delete and recreate the topic with 3 partitions:
   ```bash
   python create_topic.py
   ```
2. Run the same three producers and aggregator as in Part 1.

### What to observe

- `svc-a` **always** delivers to the same partition number (e.g. `[p0]`),
  every message, every run.
- `svc-b` and `svc-c` each consistently land on their own distinct partition.
- This is deterministic — Kafka hashes the key to pick a partition, so
  the same key always maps to the same partition for a given topic.
- All three services' logs still appear together in the aggregator
  (it subscribes to all partitions), but the per-service ordering is
  now guaranteed within each partition.

---

## Notes

- The `--level`/`--service` filtering happens **client-side** — the
  aggregator consumes every message and discards non-matching ones.
  At real log volume you'd ship into Elasticsearch, Loki, or ClickHouse
  rather than scanning the raw stream. Kafka's job is reliable, ordered
  central collection — not search.