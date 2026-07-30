# 2. Pub-Sub Fan-Out

**One event in, many independent subscribers out.**

A single `order.placed` event is published once. Three separate
"services" — Email, Analytics, Billing — each read the **entire**
stream independently, because each uses a different `group.id`.
Kafka doesn't split the messages between them (that only happens for
consumers sharing the *same* group.id) — it gives each group its own
full copy.

## Run it

1. Broker already running (`docker compose up -d` from repo root).
2. In three separate terminals:
   ```bash
   python subscriber.py --role email
   python subscriber.py --role analytics
   python subscriber.py --role billing
   ```
3. In a fourth terminal:
   ```bash
   python producer.py
   ```

## What to observe

- All three subscribers print all 5 events — nobody "steals" a message
  from anyone else.
- Check Kafka UI (`localhost:8080` → Consumers) — you'll see three
  separate consumer groups (`email-service`, `analytics-service`,
  `billing-service`), each with its own offset tracking, all reading
  the same `order-placed` topic.
- Compare this to Pattern 1: same subscribe pattern, but there it's one
  group; here it's three groups on the same topic — that's the entire
  difference between "queue-style split" and "pub-sub fan-out."