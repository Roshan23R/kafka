# 9. Data Pipelines

> **Two totally different sources. One warehouse that doesn't care where the data came from.**

Picture a real company's data platform. Orders live in MySQL. User
clicks live in an app's event stream. A data analyst wants to ask "how
does browsing behavior correlate with what people actually buy" —
except that question needs *both* datasets, sitting in the same place,
queryable together. Right now they're two separate systems that have
never met.

```text
MySQL (orders)          Mobile/web app (clicks)
     |                          |
     |  completely separate,    |
     |  never see each other    |
     v                          v
"Did the person who          "Did the order come
 bought X also click Y       from someone who
 first?" -- unanswerable,    browsed for a while
 the data lives in two       first?" -- also
 places that don't talk      unanswerable, same
 to each other                reason
```

Feeding both into one warehouse is what makes that question
answerable at all.

## 🎯 The setup

Two producers simulate real-world data sources flowing into Kafka:
`mysql_producer.py` emits order records (as a MySQL/CDC source would),
and `app_producer.py` emits user-action events (as a web/mobile app
would). The `warehouse_sink.py` consumer reads from both topics,
accumulates rows into an in-memory batch, and flushes to a local SQLite
database (`warehouse.db`) — standing in for BigQuery, Snowflake,
Redshift, or any columnar store.

```text
mysql_producer.py ──► [mysql-orders]  ──┐
                                        ├──► warehouse_sink.py ──► warehouse.db
  app_producer.py ──►  [app-events]    ─┘
```

This is the fan-in complement to Pattern 2's fan-out: instead of one
event spreading to many consumers, many sources converge into one sink.

```text
Pattern 2 (fan-out)                  Pattern 9 (fan-in)

     1 event                          many sources
        |                                |    |
   +----+----+                          v    v
   v    v    v                        ONE Kafka cluster
 many consumers                            |
 each get a copy                           v
                                       ONE sink, ONE warehouse
```

## 💡 Why batch at the sink?

Writing one Kafka message per `INSERT` to a warehouse would be
catastrophically slow — warehouse systems are optimised for bulk loads,
not individual row inserts. `warehouse_sink.py` accumulates records in
memory and only touches the database when either:

- the batch reaches **BATCH_SIZE** rows (default 50), or
- **FLUSH_INTERVAL** seconds have elapsed since the last flush (default 5s).

This is the same pattern real connectors (Kafka Connect JDBC Sink,
BigQuery Sink Connector) use internally. The tradeoff: a larger batch
means higher throughput but also more data "in flight" in RAM — if the
process crashes between flushes, those rows are lost (or re-consumed
from Kafka on the next run, depending on your offset commit strategy).

```text
one INSERT per row                one INSERT per batch

row -> INSERT -> ack               row -\
row -> INSERT -> ack               row  |
row -> INSERT -> ack               row  +--> buffer --> ONE INSERT,
...thousands of slow                row  |               many rows
round trips                        row -/
                                   handful of fast,
                                   large round trips
```

## 🧠 What "fan-in with two different schemas" actually looks like in code

Orders have `amount`. Events have `action`/`page`. Completely different
shapes, arriving on completely different topics — yet they share the
same Kafka cluster, the same consumer process, even the same flush
cycle. Kafka itself enforces nothing about schema consistency across
topics; `warehouse_sink.py` just checks `msg.topic()` and routes each
record into the batch (and eventually the table) that fits it.

```text
one warehouse_sink.py process
   |
   +-- msg.topic() == "mysql-orders" --> order_batch --> orders table
   |
   +-- msg.topic() == "app-events"   --> event_batch --> app_events table

Both batches flush together, on the SAME size/time trigger --
they don't need matching schemas to share a sink.
```

## 🏗️ Where this shows up for real

- **Customer 360 / unified analytics** — exactly this scenario:
  transactional data (orders, payments) plus behavioral data (clicks,
  sessions) landing in one warehouse for analysts to join freely.
- **Data platform ingestion layers** — most companies' "data
  engineering" teams spend a huge fraction of their time building
  exactly this: many heterogeneous sources, one warehouse, batched
  writes.
- **Multi-service reporting** — inventory, shipping, and support
  ticket systems all feeding one BI dashboard, none of them aware the
  others exist.

## ▶️ Run it

1. Broker running (`docker compose up -d` from repo root).
2. Terminal 1 — start the warehouse sink first:
   ```bash
   cd 09-data-pipelines
   python warehouse_sink.py
   ```
3. Terminal 2 — MySQL source (order records):
   ```bash
   python mysql_producer.py
   ```
4. Terminal 3 — app source (user events):
   ```bash
   python app_producer.py
   ```
   You can run either producer alone, or both at the same time.

## 👀 What to observe

- `warehouse_sink.py` prints a line each time it flushes, showing how
  many rows from each topic landed in that batch and how long since the
  last flush. When both producers are running, you'll see rows from
  both topics mixed into each flush.
- Try `--batch-size 10` for frequent small flushes, then `--batch-size 500`
  to see how batches accumulate longer before writing. The rate of
  actual DB writes changes dramatically; the rate of Kafka consumption
  stays the same.
- After stopping everything, inspect the warehouse directly:
  ```bash
  python -c "
  import sqlite3
  conn = sqlite3.connect('warehouse.db')
  print('orders:',     conn.execute('SELECT COUNT(*) FROM orders').fetchone()[0])
  print('app_events:', conn.execute('SELECT COUNT(*) FROM app_events').fetchone()[0])
  conn.close()
  "
  ```
- The `warehouse_sink.py` consumer uses `auto.offset.reset: "earliest"`,
  so if you stop and restart it, it re-reads from wherever it left off
  (tracked by the `warehouse-sink` group.id). This means the warehouse
  rows will duplicate if you restart with the same database — in
  production you'd handle this with idempotent writes or an `INSERT OR
  REPLACE` strategy keyed on `order_id` / `event_id`.
- The two topics use different schemas (orders have `amount`, events
  have `action`/`page`) yet they share the same Kafka cluster and the
  same sink process — Kafka doesn't enforce a single schema across
  topics; the sink just branches on `msg.topic()`.

## 🚀 Try breaking it

**1. Run only one producer**

Start just `mysql_producer.py`, leave `app_producer.py` off entirely.
Does `warehouse_sink.py` still flush on schedule? Does the `app_events`
table just stay empty, or does anything break?

**2. Force the duplicate-row scenario**

Stop `warehouse_sink.py`, restart it (same `warehouse.db`, same
`group.id`), and let a few more flushes happen. Query the row counts
before and after — do totals from earlier runs get double-counted?
This is the exact "at-least-once" gap called out in What to observe —
worth seeing it happen once, not just reading about it.

**3. Extreme batch settings**

Run with `--batch-size 1` (flush on every single row — watch how much
noisier and slower it gets) and separately with `--flush-interval 60`
(watch data sit invisible in the buffer for a full minute even with
plenty of rows arriving). Which extreme feels more dangerous for a
real production system, and why?

## 💬 Questions worth answering yourself

- If `mysql_producer.py` and `app_producer.py` used the *same*
  `order_id`/`event_id` numbering by coincidence, would anything in
  this pipeline break? Why does `msg.topic()` matter more here than
  the IDs themselves?
- What would you need to add to make restarts NOT duplicate rows —
  where would the "have I already written this row" check need to
  live?
- Why does `warehouse_sink.py` use one `group.id` for two topics
  instead of two separate consumers? What would you gain or lose by
  splitting it into two processes instead?

The final pattern in this set makes explicit something you've already
seen happen as a side effect in almost every earlier pattern: what
does it actually mean for a consumer to go down, come back, and pick
up exactly where it left off — with nothing lost and nothing
duplicated?

➡️ Continue to [`10-replay-recovery`](../10-replay-recovery)