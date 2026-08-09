# 5. Stream Processing

> **A single reading tells you nothing. A thousand of them, summarized, tells you everything.**

Picture a wall of sensors reporting temperature every 200 milliseconds.
One reading — "72.4" — is meaningless on its own. Was that normal? A
spike? Noise? You can't tell from one number. But the *shape* of a
thousand readings over the last five seconds — that tells a story.

```text
raw readings (noisy, individually meaningless)
72.1  73.8  71.9  90.2  91.5  89.7  72.0  71.5 ...
                    ^^^^^^^^^^^^
                 something happened here,
                 but you can't SEE it in
                 individual numbers scrolling by
```

Stream processing is what turns that noise into signal, continuously,
without ever stopping the flow to do it.

## 🎯 The setup

Raw sensor readings arrive continuously. `stream_processor.py` groups
them into 5-second **tumbling windows** — fixed, non-overlapping time
buckets — and emits ONE enriched summary per window: count, average,
min, max. This is the pattern underneath real-time dashboards,
rollups, and metrics pipelines everywhere.

```text
raw-readings (many, noisy)
   |
   v
stream_processor.py
   | groups into 5s buckets
   | closes each bucket, computes avg/min/max
   v
enriched-readings (few, clean)
   |
   v
enriched_consumer.py -- a dashboard would subscribe exactly like this
```

"3 in, 1 out" from the pattern's name is really "N in, 1 out" — the
infographic's "3" is just illustrative. How many raw readings actually
land in one window depends entirely on how fast they're arriving
relative to the window size, not on any fixed count.

## 💡 What Kafka Streams would give you for free

There's no Kafka Streams DSL available in Python, so
`stream_processor.py` hand-rolls what that DSL would normally hand
you. Worth knowing the Java equivalent, because the comments in that
file map directly to it:

```java
stream.groupByKey()
      .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
      .aggregate(...)
```

Three lines of Java become a plain Python dict (`window_start ->
list of values`) plus a manual "has this window's time passed?" check
run on every poll loop. Seeing the hand-rolled version first makes it
obvious *why* that DSL call is useful — it's quietly handling
windowing, state storage, and fault tolerance that you'd otherwise
have to write yourself.

## 🧠 The bug that's easy to miss: closing a window with no new events

A window can't close just because a message from the *next* window
arrived — what if events stop entirely for a while? Nothing would ever
trigger the close. That's why the expiry check runs on **every** poll
loop iteration, even when `msg is None` — the window closes based on
wall-clock time passing, not on message arrival.

```text
Without checking on every loop:          With checking on every loop:

events stop arriving                     events stop arriving
     |                                          |
     v                                          v
window just sits open           window still closes on schedule,
FOREVER, never emits                  because time is checked
                                       independently of messages
```

## 🏗️ Where this shows up for real

- **IoT / sensor telemetry** — exactly this scenario.
- **Clickstream rollups** — "clicks per category, every 10 seconds"
  for a live analytics dashboard.
- **Financial tick data** — raw price ticks aggregated into OHLC
  candles (open/high/low/close) for a chart.

## ▶️ Run it

1. Broker running (`docker compose up -d` from repo root).
2. Terminal 1 — the processor (the core of this pattern):
   ```bash
   python stream_processor.py
   ```
3. Terminal 2 — raw events flowing in:
   ```bash
   python raw_producer.py
   ```
4. Terminal 3 — watching only the clean output:
   ```bash
   python enriched_consumer.py
   ```

## 👀 What to observe

- `raw_producer.py` prints a noisy stream of individual readings every
  ~0.2–0.5s.
- `stream_processor.py` stays quiet most of the time, then prints
  `WINDOW CLOSED [...]` roughly every 5 seconds — that's one window's
  worth of raw readings collapsing into a single summary.
- `enriched_consumer.py` only ever sees those clean summaries — never
  the raw noise. A real dashboard would subscribe exactly like this.
- **Watch the window-closing logic carefully**: a window closes based
  on wall-clock time passing (`now_bucket > window`), not on receiving
  a message. That's why `stream_processor.py` checks for expired
  windows on *every* poll loop iteration, even when `msg is None` —
  otherwise a window with no further events would just stay open
  forever.
- **Known limitation, worth naming rather than hiding**: the `windows`
  dict is plain Python memory. If `stream_processor.py` restarts
  mid-window, that window's partial data is lost — Kafka Streams
  avoids this by backing its state store with a changelog topic, so
  it can rebuild in-flight window state after a restart. This is the
  same class of problem as Pattern 3's Redis fix, just for windowed
  state instead of a running total.

## 🚀 Try breaking it

**1. Change the window size**

In `stream_processor.py`, change `WINDOW_SECONDS = 5` to `2`. Does
`count` per window drop roughly proportionally? What does that tell
you about window size vs. event count being two independent knobs?

**2. Kill `stream_processor.py` mid-window, restart it**

Watch what happens to the partial window that was open when you
killed it. Is it in the next `WINDOW CLOSED` output, or gone entirely?

**3. Stop `raw_producer.py` for 15 seconds, then restart it**

Does `stream_processor.py` still emit empty/near-empty windows for the
gap, or does it just silently skip ahead once new data arrives?


## 💬 Questions worth answering yourself

- Why does the windowed example need a state store, but the
  transaction transform doesn't?
- Could `stream_processor.py` be rewritten as a series of 1-to-1
  transforms instead of a windowed aggregation? What would you lose by
  doing that?

The next pattern takes the "compute something continuously" idea one
step further: not just summarizing, but deciding *when to notify a
human* based on what's being computed.

➡️ Continue to [`06-metrics-alerting`](../06-metrics-alerting)