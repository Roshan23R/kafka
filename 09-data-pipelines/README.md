# 9. Data Pipelines

**Feed data lakes and warehouses from multiple sources.**

Two producers simulate real-world data sources flowing into Kafka:
`mysql_producer.py` emits order records (as a MySQL/CDC source would),
and `app_producer.py` emits user-action events (as a web/mobile app
would). The `warehouse_sink.py` consumer reads from both topics,
accumulates rows into an in-memory batch, and flushes to a local SQLite
database (`warehouse.db`) — standing in for BigQuery, Snowflake,
Redshift, or any columnar store.

```
mysql_producer.py ──► [mysql-orders]  ──┐
                                        ├──► warehouse_sink.py ──► warehouse.db
  app_producer.py ──►  [app-events]    ─┘
```

This is the fan-in complement to Pattern 2's fan-out: instead of one
event spreading to many consumers, many sources converge into one sink.

## Why batch at the sink?

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

## Run it

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

## What to observe

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
