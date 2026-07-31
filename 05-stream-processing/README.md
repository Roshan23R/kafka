# 5. Stream Processing

**Accumulate events across time windows, emit derived aggregates.**

Raw click events flow into the `clicks` topic continuously. The stream
processor buckets them into **tumbling time windows** (default 10 seconds),
counts views per product category inside each window, then emits a
summary record to `click-windows` when the window closes. A dashboard
consumer reads those summaries.

```
producer.py        processor.py                      consumer.py
 [clicks] ──► bucket by time window → aggregate ──► [click-windows]
              └─ emit summary when window closes        (dashboard)
```

This is what makes it *stream processing* rather than simple
message forwarding: state is accumulated across many events, and a
derived result is emitted at a time boundary. Production systems
(Kafka Streams, Flink, Spark Structured Streaming) add event-time
windowing, watermarks, and fault-tolerant state stores on top of
this same idea.

## Run it

1. Broker running (`docker compose up -d` from repo root).
2. Terminal 1 — raw click stream:
   ```bash
   cd 05-stream-processing
   python producer.py
   ```
3. Terminal 2 — the windowed processor:
   ```bash
   python processor.py            # 10-second windows (default)
   python processor.py --window 5 # or 5-second windows
   ```
4. Terminal 3 — dashboard consumer (optional):
   ```bash
   python consumer.py
   ```

## What to observe

- `processor.py` is silent for the first window, then prints a
  category breakdown every 10 seconds (the window closes and fires).
- The bar chart in the processor terminal shows which categories
  got the most clicks in that window.
- `consumer.py` receives the exact same summaries from the
  `click-windows` topic — it never touches the raw `clicks` topic.
- Try `--window 5` for faster feedback.

## Key concept: tumbling windows

```
time ──────────────────────────────────────────►
     |── window 1 ──|── window 2 ──|── window 3 ──|
       count events    count events    count events
       emit summary    emit summary    emit summary
```

Each window is fixed-size and non-overlapping. Every event belongs to
exactly one window. When the window closes the processor emits one
summary record and resets its counters for the next window.
