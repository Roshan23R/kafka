# Bonus: Fraud Detection

**Read → transform → write. In real time.**

Raw payment transactions flow into the `transactions` topic. A stream
processor enriches each event with a running per-user spend total and
a `high_value` flag, then writes the result to `transactions-enriched`.
A downstream alert consumer reads only from that enriched topic and
prints flagged transactions — it never touches raw data.

```
producer.py          processor.py                  consumer.py
[transactions] ──► consume → enrich → produce ──► [transactions-enriched]
                        (transform)                   (alert on high-value)
```

This **consume → transform → produce** pipeline is the core of stream
processing. Production systems use Kafka Streams, Flink, or Spark
Structured Streaming for this; here it's plain Python so the mechanics
are visible without framework overhead.

## Run it

1. Broker running (`docker compose up -d` from repo root).
2. Terminal 1 — raw event stream:
   ```bash
   cd 05-stream-processing
   python producer.py
   ```
3. Terminal 2 — the processor (reads `transactions`, writes `transactions-enriched`):
   ```bash
   python processor.py
   ```
4. Terminal 3 — downstream alert consumer:
   ```bash
   python consumer.py
   ```

## What to observe

- `processor.py` prints every transaction with a running per-user total.
  Watch the total climb as the same user's transactions accumulate.
- Transactions ≥ $200 are flagged `*** HIGH VALUE ***` in the processor
  and trigger a red `[ALERT]` line in `consumer.py`.
- `consumer.py` only reads from `transactions-enriched` — it is completely
  decoupled from the producer. New downstream consumers can be added
  without touching the processor or producer at all.
- Stop the processor mid-run and restart it — it replays from its last
  committed offset, so no transactions are skipped (running totals
  reset since they are in-memory, but no events are lost).

## Key concept

| Role | Topic | Reads from | Writes to |
|---|---|---|---|
| producer.py | raw events | — | `transactions` |
| processor.py | transformer | `transactions` | `transactions-enriched` |
| consumer.py | alert sink | `transactions-enriched` | — |

Each stage is independent. Kafka topics act as the buffer between them.
