# 4. Log Aggregation

> **Three services, three log files, one 3am page. Where do you even look?**

Picture an on-call engineer at 3am. Checkout is throwing errors.
Payments might be involved. So might the inventory service. Three
different machines, three different log files, three different
terminals SSH'd open, `grep`-ing blind, hoping the right timestamp
lines up across all of them by hand.

```text
svc-a.log (on host A)     svc-b.log (on host B)     svc-c.log (on host C)
INFO  GET /health 200     INFO  POST /pay 200       WARN  GET /cart 404
ERROR POST /pay 500       INFO  GET /user 200       INFO  POST /pay 201
INFO  GET /cart 200       ERROR POST /pay 500  <--- is this the SAME
                                                     incident as svc-a's
                                                     ERROR above? No way
                                                     to tell without
                                                     manually comparing
                                                     timestamps across
                                                     three terminals.
```

That's the problem this pattern kills.

## 🎯 The setup

Three independent services — `svc-a`, `svc-b`, `svc-c` — each emit
their own log lines continuously, completely unaware of each other.
Instead of writing to separate log files scattered across different
machines, every single one of them publishes to **one shared topic**:
`logs`.

```text
svc-a  ---\
svc-b  ----+---->  Kafka Topic "logs"  ---->  log_aggregator.py
svc-c  ---/                                    (tails everything,
                                                 can filter)
```

One aggregator tails the combined stream. The 3am engineer now has
**one place to look**, with everything already in arrival order.

## 💡 The detail that makes this actually reliable: keys

Every log line is produced with `key=args.service` — the emitting
service's own name. That's not just bookkeeping. It's what guarantees
each service's own logs stay in relative order with each other, even
though the topic has multiple partitions and even though three
producers are all writing concurrently.

```text
Topic "logs" (3 partitions)

partition 0: [svc-a] [svc-a] [svc-a] [svc-a] ...
partition 1: [svc-b] [svc-b] [svc-b] [svc-b] ...
partition 2: [svc-c] [svc-c] [svc-c] [svc-c] ...

Same key -> same partition, every single time.
That's what keeps svc-a's OWN log lines in order,
even while all three services write concurrently.
```

This repo doesn't just claim that — the "Testing" section below shows
you the actual partition number on every message, so you can watch
the hashing behave deterministically instead of taking it on faith.


## 🏗️ Where this shows up for real

- **Microservices logging** — exactly this scenario, at any company
  running more than a couple of services.
- **Container/Kubernetes logs** — every pod's stdout gets shipped
  through something like this before it ever reaches a dashboard.
- **Audit trails across systems** — security or compliance logging
  that needs a single, ordered, tamper-evident collection point.

## ▶️ Run it

1. Broker running (`docker compose up -d` from repo root).
2. **Create the topic with multiple partitions first** — by default an
   auto-created topic gets only 1 partition, and with just 1 partition
   there's nowhere else for a message to go, so you can't actually
   observe key-based routing without this step:
   ```bash
   python create_topic.py
   ```
3. Three terminals, one service each:
   ```bash
   python log_producer.py --service svc-a
   python log_producer.py --service svc-b
   python log_producer.py --service svc-c
   ```
4. A fourth terminal, tailing everything:
   ```bash
   python log_aggregator.py
   ```
   Or filtered:
   ```bash
   python log_aggregator.py --level ERROR
   python log_aggregator.py --service svc-b
   python log_aggregator.py --level ERROR --service svc-a
   ```

## Testing the "same key -> same partition" claim

Both `log_producer.py` and `log_aggregator.py` now print the partition
number for every message (`-> delivered to partition N` on the
producer side, `[pN]` on the aggregator side). With the topic created
via `create_topic.py` (3 partitions), run all three producers and
watch the output:

- Every message from `svc-a` will always show the **same** partition
  number, every time, across the entire run.
- `svc-b` and `svc-c` will each consistently show their own (likely
  different) partition number.
- This is deterministic, not random — Kafka hashes the key
  (`args.service` here) to pick a partition, so the same key always
  maps to the same partition for a given topic's partition count.

## 👀 What to observe

- All three services' logs interleave in one place, timestamped as
  they arrive — you don't need to know or care which machine/process
  produced which line.
- See "Testing the same key -> same partition claim" above to verify
  the routing behavior directly rather than taking it on faith.
- The `--level`/`--service` filtering here happens **client-side**, in
  Python, after every message is already consumed — fine for a demo,
  but at real log volume you'd typically index into something
  search-optimized downstream (Elasticsearch, Loki, ClickHouse) rather
  than filtering a raw stream by scanning every message. Kafka's job
  here is reliable, ordered central collection — not search.

## 🚀 Try breaking it

**1. Recreate the topic with 1 partition**

Delete the topic in Kafka UI (`localhost:8080`), then edit
`create_topic.py` to set `NUM_PARTITIONS = 1` and rerun it. Does
`svc-a`'s partition number ever change now? Why would that make the
"same key → same partition" test meaningless with only 1 partition?

**2. Make one service noisy, one quiet**

Kill `svc-b` and `svc-c`, leave only `svc-a` producing. Does
`log_aggregator.py --service svc-b` hang forever, error, or just sit
silently waiting? What does that tell you about the difference between
"no matching messages" and "broken"?

**3. Add a 4th service**

Run a fourth `log_producer.py --service svc-d`. No code changes
needed anywhere else — confirm it shows up in the aggregator
immediately, on whatever partition it happens to hash to.

## 💬 Questions worth answering yourself

- If `svc-a` restarted mid-run, would its logs still land on the same
  partition as before? Why or why not?
- What would break if two different services accidentally used the
  same key?
- At what point would client-side filtering in `log_aggregator.py`
  become a real performance problem — and what's the first thing
  you'd reach for instead?

The next pattern moves from "collect events as-is" to actually
**transforming** them — turning a firehose of raw data into a small
number of meaningful, derived summaries.

➡️ Continue to [`05-stream-processing`](../05-stream-processing)