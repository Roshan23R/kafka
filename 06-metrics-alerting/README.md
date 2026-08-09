# 6. Metrics & Alerting

> **The graph is climbing. Does anyone actually need to know yet?**

Picture an on-call engineer's phone buzzing every few seconds because
CPU usage ticked above 75% for one single reading, then dropped back
down, then ticked up again. By the tenth buzz in two minutes, they've
muted the channel. The next time it *really* matters, nobody's
looking.

```text
CPU%
100 |                    .  .
 90 |                  .    .    .
 80 |----------------.--------.------  <- threshold
 70 |              .            .
 60 |............                .....
    +------------------------------------> time
         ^buzz  ^buzz  ^buzz  ^buzz
       (all the SAME incident, alerted 4 times)
```

Good alerting isn't "notify on every breach." It's "notify once when
something actually changes." That's the entire problem this pattern
solves.

## 🎯 The setup

Two services (`api`, `db`) continuously emit CPU usage readings.
`alerting_processor.py` maintains a **sliding window** of the last 10
readings per source, computes a rolling average on every new reading,
and fires an alert the moment that average crosses a threshold — but,
critically, only **once** per incident, not once per breaching
reading.

```text
metrics_producer.py                alerting_processor.py
   |                                     |
   | cpu readings, api + db              | rolling avg of last 10
   v                                     v
Kafka Topic "cpu-metrics" -------->  alert once on OK->ALERT,
                                      once more on ALERT->OK,
                                      silent in between
```

## 💡 Sliding vs. tumbling (compare to Pattern 5)

- **Tumbling** (Pattern 5): fixed, non-overlapping buckets. `[0-5s]`
  closes completely, then `[5-10s]` starts from zero.
- **Sliding** (this pattern): the window is always "the last N
  readings" and moves forward one reading at a time — readings overlap
  between consecutive computations. This is why live dashboards look
  smooth instead of jumping in discrete steps.

```text
Tumbling:  [-----5s-----][-----5s-----][-----5s-----]
              closes         closes        closes
              to zero        to zero       to zero

Sliding:   [----10 readings----]
              [----10 readings----]
                 [----10 readings----]
           always overlapping, always moving by 1
```

## 🧠 Edge-triggered alerting

Naively alerting "if rolling avg >= threshold" on every reading would
fire dozens of duplicate alerts during one incident. Instead,
`alerting_processor.py` tracks `is_breaching` vs. the *previous*
reading's state, and only produces an event on the transition:
`OK -> ALERT` fires one alert; `ALERT -> OK` fires one resolved event.
Everything in between is silent. This state-machine approach is
verified in isolation — feeding a 16-reading sequence (5 normal, 5
spiking, 2 still-high, 4 recovering) through the same logic produces
exactly 2 events (1 ALERT + 1 RESOLVED), not 7.

```text
readings:    20  25  30  90  92  88  95  91  89  93  20  18
breaching:    N   N   N   N   N   Y   Y   Y   Y   Y   N   N
                                  ^                   ^
                               ALERT               RESOLVED
                             (fires once)         (fires once)

  7 breaching readings. 2 alerts fired. Not 7.
```

This same shape — "notify on change, not on every tick" — shows up
everywhere outside Kafka too: hardware debouncing, CI pass/fail
notifications, UI state changes. Worth recognizing it as a general
pattern, not a Kafka-specific trick.

## 🏗️ Where this shows up for real

- **Infrastructure monitoring** — Datadog, Prometheus alerting rules,
  PagerDuty — all built around exactly this "rolling window +
  edge-trigger" shape.
- **Fraud detection** — a spike in failed transactions triggers one
  investigation ticket, not one per failed transaction.
- **IoT equipment health** — a sensor trending toward a dangerous
  range fires one maintenance alert, not a flood.

## ▶️ Run it

1. Broker running (`docker compose up -d` from repo root).
2. Terminal 1 — the alerting processor:
   ```bash
   python alerting_processor.py
   ```
3. Terminal 2 — metrics flowing in:
   ```bash
   python metrics_producer.py
   ```
4. Terminal 3 (optional) — the "on-call notifier":
   ```bash
   python alert_consumer.py
   ```

## 👀 What to observe

- Most of the time, `alerting_processor.py` just prints the rolling
  average quietly — no alert.
- Every so often (the producer randomly triggers a ~5-8 second spike
  period, ~3% chance per tick, per source), you'll see rolling average
  climb, then `!!! ALERT`, then a run of high readings with **no**
  further alerts, then `✓ RESOLVED` once it drops back down.
- `alert_consumer.py` only ever sees the `ALERT`/`RESOLVED` events —
  never the raw noisy metric stream — same "subscribe to the filtered
  output, not the firehose" idea as Pattern 5's alert topic.
- Notice `alerting_processor.py` uses `auto.offset.reset: "latest"`,
  unlike every other pattern in this repo which uses `"earliest"`.
  This is deliberate: an alerting system generally cares about *live*
  state going forward, not replaying a backlog of old metrics from
  before it started — replaying old spikes on startup would trigger
  false alerts for incidents that are long over.

## 🚀 Try breaking it

**1. Lower the threshold**

In `alerting_processor.py`, change `ALERT_THRESHOLD = 75.0` to `40.0`.
Do you now see far more frequent, shorter-lived alerts? What does that
tell you about tuning threshold vs. tuning window size?

**2. Shrink the window**

Change `WINDOW_SIZE = 10` to `3`. Does the rolling average become more
twitchy — reacting faster but flapping between ALERT/RESOLVED more
often? That's the smoothness-vs-responsiveness tradeoff every
monitoring system has to tune.

**3. Start `alerting_processor.py` *after* a spike has already happened**

Run `metrics_producer.py` alone for 15 seconds first, then start
`alerting_processor.py`. Because of `auto.offset.reset: "latest"`,
does it alert on the spike that already happened, or only on what
happens from here forward?

## 💬 Questions worth answering yourself

- Why does a bigger `WINDOW_SIZE` make alerts slower to fire *and*
  slower to resolve? Is that a coincidence, or the same mechanism
  working in both directions?
- What would happen to alert quality if `alerting_processor.py` used
  `auto.offset.reset: "earliest"` instead of `"latest"`?
- Could you build this same edge-trigger idea directly into
  `metrics_producer.py` instead of a separate processor? What would
  you lose by doing that?

The next pattern leaves "watching a live stream" behind entirely and
asks a very different question: what if the event log itself — not a
database — is the actual source of truth for your application's
state?

➡️ Continue to [`07-event-sourcing`](../07-event-sourcing)