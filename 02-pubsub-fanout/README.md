# 2. Pub-Sub Fan-Out

> **One event. Three teams. Nobody steals anybody else's copy.**

Picture the moment an order gets placed.

The order service doesn't want to know or care who's downstream. But
in reality, at least three separate teams need to hear about it:

- **Email** wants to send a confirmation.
- **Analytics** wants to record it for the revenue dashboard.
- **Billing** wants to charge the card.

None of these teams should have to coordinate with each other. None
of them should have to fight over who "gets" the event. All three
need the *same* event, independently, in full.

```text
                     order.placed
                         |
                         v
                  Order API publishes
                         |
                         v
              +----------+----------+
              |          |          |
              v          v          v
           Email    Analytics   Billing
         (send it)  (log it)   (charge it)
```

## 🎯 The problem this solves

If Kafka only supported "one message, one consumer" — like a plain
task queue — you'd have a fight on your hands. Whichever service
happened to poll first would grab the message, and the other two
would get nothing. You'd need to build your own fan-out logic on top,
copying the message into three separate queues yourself.

Kafka does this natively, and the mechanism is something you already
met in Pattern 1: **`group.id`**.

## 💡 The one-line trick

```text
subscriber.py --role email        -> group.id = "email-service"
subscriber.py --role analytics    -> group.id = "analytics-service"
subscriber.py --role billing      -> group.id = "billing-service"
```

Three different group IDs, subscribed to the *same* topic. That's the
entire mechanism:

```text
Kafka Topic "order-placed"
+------------------------+
|  E0  E1  E2  E3  E4    |
+------------------------+
   |        |        |
   v        v        v
email-   analytics- billing-
service   service    service
(group)   (group)    (group)

Each group tracks ITS OWN offset.
Each group sees ALL 5 events.
```

Compare that to Pattern 1, where a *second* consumer using the same
`group.id` would have **split** the work instead of duplicating it.
Same topic, same `subscribe()` call — the only difference is whether
the group.id matches or not. That one string is the entire dividing
line between "queue-style load balancing" and "pub-sub fan-out."

## 🧠 Why this actually matters

Without this, adding a new downstream service would mean touching the
producer's code — "oh, now I also need to notify Fraud Detection, let
me go add another call in the order API." With Kafka, adding Fraud
Detection later means: write a new consumer, give it its own
`group.id`, subscribe to `order-placed`. The order API is never
touched. It has no idea Fraud Detection exists.

```text
Adding a 4th subscriber later:

Order API   -----unchanged----->   Kafka Topic
                                        |
                            +-----------+-----------+-----------+
                            v           v            v          v
                          Email    Analytics     Billing   Fraud (NEW)
```

That's the real payoff: **producers and the set of consumers are
completely decoupled**. You can add, remove, or rewrite a subscriber
without anyone else noticing.

## 🏗️ Where this shows up for real

- **E-commerce**: order placed → email, analytics, billing, fraud
  check, inventory reservation — all independent.
- **User signup**: new account created → welcome email, CRM sync,
  onboarding email drip, analytics — all reading the same event.
- **Video upload**: video published → transcoding, thumbnail
  generation, content moderation, notify subscribers — same shape
  again.

The pattern doesn't change. Only the names of the subscribers do.

## ▶️ Run it

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

## 👀 What to observe

- All three subscribers print all 5 events — nobody "steals" a message
  from anyone else.
- Check Kafka UI (`localhost:8080` → Consumers) — you'll see three
  separate consumer groups (`email-service`, `analytics-service`,
  `billing-service`), each with its own offset tracking, all reading
  the same `order-placed` topic.
- Compare this to Pattern 1: same subscribe pattern, but there it's one
  group; here it's three groups on the same topic — that's the entire
  difference between "queue-style split" and "pub-sub fan-out."

## 🚀 Try breaking it

**1. Give two subscribers the same role**

Run `python subscriber.py --role email` in two terminals *at the same
time*. What happens to the 5 events — does each terminal print all of
them, or does the work split between the two? (This is Pattern 1's
behavior sneaking back in — same group.id, two members.)

**2. Add a fourth "service"**

Add a new key to the `ACTIONS` dict in `subscriber.py` — say,
`"fraud"` — and run `python subscriber.py --role fraud`. No changes
needed anywhere else. Does it get the full stream too?

**3. Stop one subscriber, then produce more events, then restart it**

Does it pick up only what it missed, or does it re-read everything?

## 💬 Questions worth answering yourself

- What's the one config value that separates "split the work" from
  "duplicate the work"?
- If you delete a subscriber's `group.id` entirely from Kafka's memory
  (e.g. by inventing a brand-new group name), what does it see when it
  first connects?
- Could you build this same fan-out behavior with three separate
  topics instead of three consumer groups on one topic? What would you
  lose by doing it that way?

The next pattern takes this one step further: what happens when a
consumer needs to *remember something across many events*, not just
react to each one individually?

➡️ Continue to [`03-activity-tracking`](../03-activity-tracking)