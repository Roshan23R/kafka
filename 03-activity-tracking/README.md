# 3. Activity Tracking

> **A dashboard that forgets everything the moment you restart it.**

Picture a product manager watching a live "clicks right now" dashboard
during a launch. Every page view, every click, ticking up in real
time. Looks great — until someone redeploys the service and the
counter silently drops back to zero, even though nothing was actually
lost.

That's the trap this pattern walks you straight into, on purpose.

```text
website
   |
   | click, page_view, add_to_cart, scroll...
   v
 Kafka Topic "user-clicks"
   |
   v
consumer.py --> tally in memory --> print dashboard
```

## 🎯 The setup

`producer.py` simulates a website continuously streaming click and
page-view events — the kind of thing a real frontend would send to a
`/track` endpoint that forwards into Kafka.

The interesting part isn't the producer. It's that this folder ships
**two different consumers**, on purpose, so you can watch the same bug
happen and then watch it get fixed:

```text
consumer.py                          consumer_redis.py
   |                                     |
   | counts live in a                    | counts live in Redis
   | plain Python dict                   | (durable, external)
   v                                     v
restart -> back to zero        restart -> "Loaded N existing
                                           events from Redis"
```

## 💡 The trap: Kafka remembers, your variables don't

Here's the part that catches people out. `consumer.py` uses a real
`group.id`, so Kafka *does* correctly remember which messages it has
already read — restart it, and it won't reprocess old events. And yet
the dashboard still resets to zero.

Why? Because "how many clicks have I seen" was never stored anywhere
Kafka manages. It only ever lived in a Python variable, in RAM, in
that one process. Kafka faithfully protects your **position in the
log** — it has no idea, and no way to know, that you were also
keeping a private tally on the side.

```text
What Kafka remembers:            What your code remembers:
"offset 47, already read"        total = 812   (just a variable)
        |                                |
        v                                v
   survives restart               dies with the process
```

`consumer_redis.py` fixes this by writing the running totals to Redis
on every event, and *loading* them back on startup — so the aggregate
becomes just as durable as Kafka's own offset tracking.


## 🏗️ Where this shows up for real

- **Any "live count" dashboard** — active users, requests/sec, cart
  additions during a flash sale.
- **Rate limiting / abuse detection** — "how many failed logins for
  this account in the last N minutes" needs to survive a restart of
  the service checking it.
- **Leaderboards** — a live-updating score board that resets itself
  every deploy is not a leaderboard anyone trusts.

The lesson generalizes past Kafka entirely: **any in-memory
aggregate is one restart away from silently lying to you**, no matter
how reliable the system feeding it is.

## ▶️ Run it

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

## 👀 What to observe

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

## 🚀 Try breaking it

**1. Kill `consumer.py` mid-stream, restart it, compare to `consumer_redis.py`**

Run both side by side (different terminals) against the same producer
run. Restart each in turn. Watch one lie to you and one tell the
truth.

**2. Kill `consumer_redis.py` at the exact wrong moment**

Hard to time by hand, but worth reasoning through: if you could freeze
time right after the Redis write but right before the code moves on
to the next message, and the process died there — what would happen
on restart? Would that event get processed once, or twice?

**3. Delete the Redis keys manually, then restart `consumer_redis.py`**

```bash
docker exec -it redis redis-cli DEL activity:total activity:per_page activity:per_type
```
What does it print on startup now? Does it behave like `consumer.py`
in this case?

## 💬 Questions worth answering yourself

- What, precisely, does Kafka guarantee will survive a restart — and
  what does it *not* guarantee?
- Why doesn't `auto.offset.reset: earliest` cause `consumer.py` to
  replay the whole topic every time you restart it?
- If you were designing this for real, would you rather have the
  Redis write happen *before* or *after* the Kafka offset commit? What
  does each ordering fail toward — double-counting, or under-counting?

The next pattern moves from "one service's clicks" to "many services'
logs, all landing in one place" — a different shape of aggregation,
where the interesting question becomes filtering and ordering instead
of running totals.

➡️ Continue to [`04-log-aggregation`](../04-log-aggregation)