# 7. Event Sourcing

> **What's your account balance? There's no field that says. You have to ask history.**

Picture a bank statement. It doesn't store one number called "your
balance." It stores every deposit, every withdrawal, every fee — a
list. Your balance is what you get when you add all of those up.
Nobody at the bank ever "sets" your balance directly; it only ever
exists as the *sum of everything that happened*.

```text
Normal database thinking:              Event-sourced thinking:

accounts table                         account-events log
+------------+---------+               +------------------+
| account_id | balance |               | DEPOSIT  +100    |
+------------+---------+               | WITHDRAW -30     |
| acc-1      |   90    |  <- one       | DEPOSIT  +20     |
+------------+---------+     number,   +------------------+
                              history          |
                              overwritten      v
                              on every UPDATE  fold them all together
                                               -> 90 (derived, not stored)
```

This pattern is that bank-statement mental model, applied to Kafka.

## 🎯 The setup

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

## 💡 Why anyone would want this

A normal `UPDATE` throws away the "how did we get here." Event
sourcing keeps it, for free, as a side effect of how storage works.
That has real, practical payoffs:

```text
"Why is this account's balance $90?"

Normal DB:        "It just is. That's what the row says."

Event-sourced:     DEPOSIT +100  (Jan 3, 9:14am)
                    WITHDRAW -30  (Jan 3, 2:30pm)
                    DEPOSIT +20  (Jan 5, 11:02am)
                    -----------------------------
                    = $90, and here's exactly how
                      we got there, in order
```

You get a full audit trail without building one separately. You can
answer "what did the balance look like last Tuesday at noon" by
replaying only up to that point. And — this is the part `rebuild_balance.py`
demonstrates directly — if your derived balance ever gets corrupted or
you change how you compute it, you can throw it away and rebuild it
perfectly from the log, because the log never lied to you in the
first place.

## 🧠 The subtlety: does replay order actually matter here?

`rebuild_balance.py` uses **manual partition assignment and watermark
offsets** rather than a normal subscribe-and-run consumer, because
this is a one-shot batch replay that needs a hard, known finish line —
not a service that runs forever. Worth reasoning through *why* the
partition count doesn't threaten correctness here: since every event
is keyed by `account_id`, one account's own events always land on the
same partition, in order — no matter how many partitions exist or how
other accounts interleave around them. And because summing
deposit/withdrawal deltas is commutative, even cross-account
interleaving order wouldn't change the final numbers. Order would only
start to matter for logic that depends on sequence — e.g. rejecting a
withdrawal based on the balance *at that specific moment* — which this
demo doesn't implement, but is the exact line where partitioning
choices start to matter for real.

## 🏗️ Where this shows up for real

- **Banking & ledgers** — literally this example; double-entry
  bookkeeping is event sourcing that predates computers by centuries.
- **Version control** — Git doesn't store "the current file." It
  stores every commit and derives the current state by replaying them.
- **Shopping carts / order state machines** — `ITEM_ADDED`,
  `ITEM_REMOVED`, `CHECKED_OUT` as the log, current cart contents as
  the derived view.

## ▶️ Run it

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

## 👀 What to observe

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

## 🚀 Try breaking it

**1. Produce events for a brand-new account**

Add a new account name to `ACCOUNTS` in `events_producer.py`, produce
some events for it, then rerun `rebuild_balance.py`. Does it show up
automatically, with no code changes to the rebuild script?

**2. Interrupt `rebuild_balance.py` mid-replay**

Ctrl+C it partway through a large replay, then run it again. Does it
resume from where it stopped, or start over completely? Why?

**3. Try to make it resume instead of replay**

Based on the earlier discussion of `.assign()` + `.seek(0)` always
forcing a full replay: what specific lines would you need to remove or
change to make this script *resume* from last time instead of
replaying everything? (Hint: think about what would need to be
persisted between runs, and revisit Pattern 3's Redis approach.)

## 💬 Questions worth answering yourself

- If you deleted the entire `account-events` topic and lost all its data, is
  there any way to recover the current balances? What does your answer
  say about where "the truth" actually lives in this design?
- Why does `rebuild_balance.py` need to know the exact partition count
  and watermark offsets, when `events_producer.py` never needs to
  think about partitions at all?
- What would break in this design if two different account IDs
  accidentally hashed to a scenario where their events needed to stay
  interleaved in a specific order relative to each other?

The next pattern moves from "one system's internal log" to "keeping a
totally separate system in sync automatically" — no application code
required on the syncing side at all.

➡️ Continue to [`08-cdc`](../08-cdc)