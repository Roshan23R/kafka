# 6. Metrics & Alerting

**Live telemetry with instant alerts.**

Two services (`api`, `db`) continuously emit CPU usage readings.
`alerting_processor.py` maintains a **sliding window** of the last 10
readings per source, computes a rolling average on every new reading,
and fires an alert the moment that average crosses a threshold — but
only once per incident, not once per breaching reading.

## Sliding vs. tumbling (compare to Pattern 5)

- **Tumbling** (Pattern 5): fixed, non-overlapping buckets. `[0-5s]`
  closes completely, then `[5-10s]` starts from zero.
- **Sliding** (this pattern): the window is always "the last N
  readings" and moves forward one reading at a time — readings overlap
  between consecutive computations. This is why live dashboards look
  smooth instead of jumping in discrete steps.

## Edge-triggered alerting

Naively alerting "if rolling avg >= threshold" on every reading would
fire dozens of duplicate alerts during one incident. Instead,
`alerting_processor.py` tracks `is_breaching` vs. the *previous*
reading's state, and only produces an event on the transition:
`OK -> ALERT` fires one alert; `ALERT -> OK` fires one resolved event.
Everything in between is silent.

## Run it

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

## What to observe

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