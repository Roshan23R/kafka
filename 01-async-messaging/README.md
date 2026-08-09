# 1. Async Messaging

> **The API shouldn't have to wait for slow work.**

Imagine you're building an order service.

A customer places an order. The order itself is simple — a customer,
an amount, done. But *after* that, several things need to happen:

- send a confirmation
- update analytics
- kick off billing
- maybe more, later

If your order API waits for every one of those to finish before
responding, the customer sits there staring at a spinner while your
billing service does something slow and unrelated to "did the order
go through."

```text
Customer
   |
   |  POST /order
   v
Order API -----> do billing work -----> respond
                  (slow, unrelated
                   to "order placed")
```

The API shouldn't have to know or care how slow billing is. That's
the problem this pattern solves.

## 🎯 The setup

In this demo, `producer.py` plays the role of the order API: it fires
20 order events as fast as it possibly can, no throttling.

`consumer.py` plays the role of a slow downstream worker: it
deliberately takes 0.5 seconds to "process" each one (standing in for
a real DB write, an email send, a call to another service).

```text
producer.py                          consumer.py

20 orders                          0.5s / order
    |                                    |
    v                                    v
+----------+                       +------------+
| produce  | ----> Kafka Topic --> |  consume   |
| quickly  |      "orders"         |   slowly   |
+----------+                       +------------+
```

Kafka's topic sits between them as a buffer. The producer doesn't wait
around — it dumps all 20 orders into the topic and exits. The consumer
picks them up at whatever pace it can actually manage.

## 💡 The pattern underneath

```text
                    Kafka Topic "orders"
                 +----------------------+
                 | O0 O1 O2 O3 ... O19  |
                 +----------------------+
                   ^                |
                   |                |
              producer.py      consumer.py
               (fast)            (slow)
```

The producer doesn't need to know:
- how fast the consumer is
- when the consumer will actually get to a given order
- whether the consumer is currently behind

It just publishes. The consumer reads at its own pace, whenever it's
ready. Neither side is blocked by the other.

## 🔥 What this actually demonstrates

Producer speed and consumer speed don't have to match — at all.

```text
producer.py:   20 orders in     ~1 second
consumer.py:    1 order every    0.5 seconds
consumer.py total time:        ~10 seconds
```

The producer is done in a second. The consumer is still working
through the backlog nine seconds later. Nobody crashed, nothing was
lost — Kafka just held the difference.

## 🧠 The interesting part: what if the consumer dies mid-way?

Say `consumer.py` has processed 8 of the 20 orders, then you kill it
(`Ctrl+C`, or it crashes for real). Restart it.

It does **not** start over from order 0, and it does **not** lose the
remaining 12. It picks up exactly where it left off:

```text
Partition (topic "orders")

 O0  O1  O2  O3  O4  O5  O6  O7  O8  O9 ...
                              ^
                              |
                       committed offset
```

That remembered position — the **offset** — is what makes Kafka more
than an in-memory queue. A plain in-process queue dies with the
process. Kafka's topic is a durable log sitting outside your
application entirely, so a crash doesn't erase your place in line.

## 🏗️ Where this shows up for real

This exact shape — fast producer, slow decoupled consumer — is
everywhere once you start looking:

```text
Order Processing                 Image/Video Upload            Notifications
Order API                        Upload API                    Application
   |                                |                              |
   | OrderPlaced                    v                              v
   v                              Kafka                          Kafka
 Kafka                              |                              |
   |                                v                              v
   +--> Billing                  Worker                    Notification Worker
   +--> Analytics                  |                              |
   +--> Notifications              +--> resize                (sends email/push,
                                    +--> compress                doesn't block
                                    +--> thumbnail                the request)
```

## 🤔 Why not just call the consumer directly?

```text
Without Kafka                       With Kafka

producer.py                         producer.py
    |                                    |
    | direct call (HTTP/RPC)             | publish
    v                                    v
consumer.py                           Kafka
    |                                    |
    | slow                               | consume, whenever ready
    v                                    v
response                            consumer.py
```

Calling directly ties the two together — if the consumer is slow or
down, the producer feels it immediately. Going through Kafka
decouples them: each side can be scaled, restarted, or rewritten
without the other one needing to know.

## ⚠️ One thing this pattern does *not* do

Async messaging doesn't make the work disappear. Someone still has to
process those 20 orders — Kafka just separates *producing* the event
from *processing* it, so the producer never has to wait around for
that processing to finish. That separation is what gives the system
room to absorb bursts.

## ▶️ Run it

1. Start the broker (from repo root):
   ```bash
   docker compose up -d
   ```
2. In one terminal:
   ```bash
   python consumer.py
   ```
3. In another terminal:
   ```bash
   python producer.py
   ```

## 👀 What to observe

- `producer.py` finishes in under a second even though it sent 20 messages.
- `consumer.py` is still working through the backlog 10 seconds later.
- Kill the consumer mid-way and restart it — it resumes from where it
  left off (consumer group offset), no messages lost.

## 🚀 Try breaking it

Once the basic example works, poke at it.

**1. Make the consumer even slower**

In `consumer.py`, change:
```python
time.sleep(0.5)
```
to:
```python
time.sleep(2)
```
What happens to the backlog? Does the producer still finish first?

**2. Produce a lot more**

In `producer.py`, change `range(20)` to `range(1000)`. Does the
producer still finish well before the consumer catches up?

**3. Run two consumers at once**

Start a second one:
```bash
python consumer.py
```
What happens to the 20 messages — do both consumers process all of
them, or does the work split between the two? (Hint: this is the
exact question Pattern 2 answers.)

## 💬 Questions worth answering yourself before moving on

- Why doesn't the producer wait for the consumer?
- Where do the messages actually live while the consumer is behind?
- What is an offset, concretely?
- What happens on a consumer crash — does anything get lost?
- What happens if you run two consumers with the same `group.id`?
- Is Kafka giving you a queue, a log, or both?

The next pattern builds directly on this and asks a different
question: what happens when *multiple independent services* all need
to see the same event, not just one worker splitting the load?

➡️ Continue to [`02-pubsub-fanout`](../02-pubsub-fanout)