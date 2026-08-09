"""
Pattern 9: Data Pipelines
--------------------------
The warehouse sink: consumes from both 'mysql-orders' and 'app-events',
accumulates records into an in-memory batch, then flushes to a local
SQLite file ('warehouse.db') once the batch reaches BATCH_SIZE rows or
FLUSH_INTERVAL seconds have elapsed -- whichever comes first.

SQLite stands in for a real warehouse (BigQuery, Snowflake, Redshift).
The batching logic is the same regardless of destination: writing one
Kafka message per INSERT would saturate even a fast warehouse; batching
amortizes the round-trip cost across many rows per write.

Run:
    python warehouse_sink.py
    python warehouse_sink.py --batch-size 100     # flush every 100 rows
    python warehouse_sink.py --flush-interval 10  # or every 10 seconds
"""

import json
import time
import sqlite3
import argparse
from confluent_kafka import Consumer

TOPICS  = ["mysql-orders", "app-events"]
DB_FILE = "warehouse.db"


def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id    INTEGER,
            customer_id TEXT,
            product     TEXT,
            amount      REAL,
            created_at  REAL,
            ingested_at REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS app_events (
            event_id    INTEGER,
            user_id     TEXT,
            action      TEXT,
            page        TEXT,
            ts          REAL,
            ingested_at REAL
        )
    """)
    conn.commit()


def flush_batch(conn, order_batch, event_batch):
    if order_batch:
        conn.executemany("INSERT INTO orders VALUES (?,?,?,?,?,?)", order_batch)
    if event_batch:
        conn.executemany("INSERT INTO app_events VALUES (?,?,?,?,?,?)", event_batch)
    conn.commit()
    return len(order_batch), len(event_batch)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size",     type=int,   default=50,  help="Flush after this many rows (default 50)")
    parser.add_argument("--flush-interval", type=float, default=5.0, help="Flush after this many seconds (default 5)")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_FILE)
    init_db(conn)

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id":          "warehouse-sink",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(TOPICS)

    order_batch, event_batch = [], []
    total_orders = total_events = 0
    last_flush = time.time()

    print(f"Warehouse sink running  batch={args.batch_size} rows  interval={args.flush_interval}s  db={DB_FILE}")
    print("Run mysql_producer.py and/or app_producer.py in separate terminals.\n")

    try:
        while True:
            msg = consumer.poll(0.1)

            if msg is not None and not msg.error():
                now    = time.time()
                record = json.loads(msg.value())

                if msg.topic() == "mysql-orders":
                    order_batch.append((
                        record["order_id"],   record["customer_id"],
                        record["product"],    record["amount"],
                        record["created_at"], now,
                    ))
                else:
                    event_batch.append((
                        record["event_id"], record["user_id"],
                        record["action"],   record["page"],
                        record["ts"],       now,
                    ))

            batch_total = len(order_batch) + len(event_batch)
            elapsed     = time.time() - last_flush

            should_flush = batch_total >= args.batch_size or (
                batch_total > 0 and elapsed >= args.flush_interval
            )

            if should_flush:
                n_orders, n_events = flush_batch(conn, order_batch, event_batch)
                total_orders += n_orders
                total_events += n_events
                print(
                    f"Flushed {n_orders:>4} orders + {n_events:>4} events  "
                    f"(total: {total_orders} orders, {total_events} events)  "
                    f"elapsed={elapsed:.1f}s"
                )
                order_batch, event_batch = [], []
                last_flush = time.time()

    except KeyboardInterrupt:
        pass
    finally:
        n_orders, n_events = flush_batch(conn, order_batch, event_batch)
        total_orders += n_orders
        total_events += n_events
        consumer.close()
        conn.close()
        print(f"\nFinal flush. Written to {DB_FILE}: {total_orders} orders, {total_events} app events.")


if __name__ == "__main__":
    main()
