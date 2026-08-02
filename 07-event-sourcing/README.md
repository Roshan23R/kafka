# 7. Event Sourcing

**State as an append-only log.**

`events_producer.py` emits `DEPOSIT`/`WITHDRAW` events for a couple of
accounts. Notice what's deliberately *not* there: no "current balance"
field, no row being updated anywhere. Each event only describes a
**change** — the sequence of events itself, `account-events`, is the
source of truth. Current balance is a **derived value**, computed by
replaying the entire log and folding every event into a running total
— that's what `rebuild_balance.py` does.

This is the opposite mental model from a normal database table (where
you'd `UPDATE accounts SET balance = balance + 50`, overwriting the
old value and losing history). Here, nothing is ever overwritten —
the full history of *how* the balance got to where it is stays
available forever.

## Run it

1. Broker running (`docker compose up -d` from repo root).
2. Create the topic with multiple partitions:
   ```bash
   python create_topic.py
   ```
   See "Why multiple partitions is safe here" below for why this
   doesn't break correctness.
3. Let some events accumulate:
   ```bash
   python events_producer.py
   ```
   Let it run for 20-30 seconds, then Ctrl+C.
3. Rebuild balances from the full log:
   ```bash
   python rebuild_balance.py
   python rebuild_balance.py --account acc-1
   python rebuild_balance.py --verbose   # watch every event replay
   ```

## Why multiple partitions is safe here

Event sourcing correctness doesn't require the *whole topic* to be
strictly ordered — only that each **account's own events** stay
ordered relative to each other. Since every event is keyed by
`account_id`, Kafka guarantees all of one account's events land in the
same partition, in order, regardless of how many partitions the topic
has or how other accounts' events interleave around them.
`rebuild_balance.py` already reads across every partition when
replaying (see `list_topics` + assigning all partitions), so this
holds with no code changes. Worth knowing the deeper reason this
specific demo is extra safe: summing deposit/withdrawal deltas is
commutative, so the *final* balance would be identical even if two
different accounts' events interleaved in any order. Order only
starts to matter for logic that depends on sequence — e.g. rejecting
a withdrawal based on the balance *at that moment* — which this simple
demo doesn't implement, but is worth knowing as the line where
partitioning choices would start to matter more.

## What to observe

- `rebuild_balance.py` always starts from **offset 0** — the very
  first event ever produced — and replays forward. Run it twice in a
  row with no new events in between: you get the exact same balances
  both times, computed from scratch each time.
- Run `events_producer.py` again to add more events, then run
  `rebuild_balance.py` again — the balances reflect *everything*,
  old and new, because the whole log gets replayed every time.
- With `--verbose`, you can watch the running balance change
  event-by-event, in the exact order those events actually happened —
  this only works correctly because events are keyed by `account_id`
  (see `events_producer.py`), which keeps one account's events in
  order within their partition.
- **The real-world tradeoff this exposes**: replaying the *entire*
  history every single time you want "the current balance" is fine
  for a demo with a few dozen events, but doesn't scale — a real
  event-sourced system periodically saves a **snapshot** (e.g. "as of
  event #50,000, balance was $312.40") so it only needs to replay
  events *since* the last snapshot, not from the very beginning. This
  is the same idea as Pattern 3's Redis persistence — avoiding a full
  recompute on every read — applied to a full event history instead
  of a simple counter.
- `rebuild_balance.py` uses **manual partition assignment and
  watermark offsets** (`consumer.assign()`, `get_watermark_offsets()`)
  instead of `consumer.subscribe()` with a consumer group — worth
  understanding why: this is a one-shot batch job that needs to know
  precisely when it has reached "the end" and stop, not a long-running
  service that tracks its position across restarts via a shared
  group.id. Different job, different consumer API.