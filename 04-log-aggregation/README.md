# 4. Log Aggregation

**All service logs in one stream.**

Three independent services (`svc-a`, `svc-b`, `svc-c`) each emit their
own log lines continuously. Instead of writing to separate log files
scattered across machines, they all publish to one shared `logs`
topic. A single aggregator then tails the combined stream — and can
filter it, similar to a simple "search all logs" box.

## Run it

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

## What to observe

- All three services' logs interleave in one place, timestamped as
  they arrive — you don't need to know or care which machine/process
  produced which line.
- Each log line is keyed by service name (`key=args.service`) — this
  means Kafka would route all of one service's logs to the same
  partition if the topic had multiple partitions, which keeps that
  service's own logs in order relative to each other.
- The `--level`/`--service` filtering here happens **client-side**, in
  Python, after every message is already consumed — fine for a demo,
  but at real log volume you'd typically index into something
  search-optimized downstream (Elasticsearch, Loki, ClickHouse) rather
  than filtering a raw stream by scanning every message. Kafka's job
  here is reliable, ordered central collection — not search.