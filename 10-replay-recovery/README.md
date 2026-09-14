# 10. Replay & Recovery

> **A consumer goes down for ten minutes. When it wakes up, does anything actually go wrong?**

Picture a service that processes payment events. It runs fine for
weeks, then a deploy goes bad, the pod crashes, and it's down for ten
minutes before Kubernetes restarts it. In those ten minutes, forty
payments happened. What does the service see when it comes back?

```text
Without a durable log (e.g. a plain in-memory queue):

producer keeps sending -----> [service is DOWN] -----> those 40 events
                                                          are just GONE.
                                                          Nobody was
                                                          listening.
With Kafka:

producer keeps sending -----> [service is DOWN] -----> events sit safely
                                     |                  in the topic,
                                     v                  retained, waiting
                              service restarts
                                     |
                                     v
                        picks up EXACTLY where it left off,
                        processes all 40, catches up completely
```

Every other pattern in this repo has *relied* on this property without
making it the main event. This pattern makes it the main event.

## 🎯 The setup

`events_producer.py` emits a steady stream of numbered events — nothing
fancy, just enough to have something running continuously so you can
watch a consumer fall behind and catch up.

`resilient_consumer.py` is the core of this pattern. It commits its
offset **manually**, every 5 messages — not after every single one
(the safest option, but slower), and not never (fast, but nothing is
ever remembered). This is the real-world tradeoff every consumer
built for throughput has to make, and this script makes that tradeoff
visible instead of hiding it:

```text
resilient_consumer.py

process msg -> process msg -> process msg -> process msg -> process msg
                                                                  |
                                                                  v
                                                            COMMIT (every 5th)
```

You can stop it two different ways, on purpose, to see two different
outcomes:

```text
Ctrl+C (graceful)                    --crash-after N (simulated crash)

finally block runs                   os._exit() -- skips the finally
  |                                  block entirely, exactly like a
  v                                  real crash (OOM kill, power loss)
commits whatever's left                    |
  |                                        v
  v                                  whatever was processed since the
restart reprocesses                  LAST COMMIT is un-saved
  NOTHING                                  |
                                           v
                                     restart REPROCESSES those messages
                                     (at most 4, since we commit every 5)
```

Nothing is ever silently **lost** either way. In the worst case (a hard
crash right before a commit was due), a handful of messages get
processed *twice* — never zero times. That's Kafka's default
**at-least-once** guarantee, made concrete instead of abstract.

`replay_tool.py` is a separate, standalone tool for a different job:
deliberately replaying a *specific* window of history — not "resume
where a consumer left off," but "let me choose exactly where to start
reading, for debugging or backfilling." You can start from the
beginning, from a specific offset, or from a timestamp ("replay
everything from 2:14pm onward").

## 💡 Why "replay from any offset," not just "replay from 0"

Pattern 7 always restarted from offset 0 — a full, fixed replay every
time. That's the right choice when you need the *entire* history to
compute something (a balance). But a lot of real recovery/debugging
scenarios need a much narrower question answered: "our alerting
service had a bug between 2:14pm and 2:20pm — replay exactly that
six-minute window so we can reprocess it correctly, without touching
anything before or after."

```text
Pattern 7's replay:              Pattern 10's replay:

ALWAYS starts at 0            starts WHEREVER you choose
   |                                    |
   v                                    v
full history,                  --from-offset 20
every single time              --from-timestamp 1735000000
                                --from-beginning (same as before,
                                 now just one option among several)
```

`replay_tool.py` resolves a timestamp to an exact offset using Kafka's
own `offsets_for_times()` lookup — it asks the broker directly "what's
the offset of the first message at or after this moment," rather than
scanning from the beginning yourself to find it by hand.

## 🧠 Verifying the math, not just trusting it

I tested the commit-interval logic directly, independent of Kafka:
simulating a crash exactly on a commit boundary (10 messages, commits
at 4 and 9) correctly reprocesses **0** messages on restart; a crash
2 messages past the last commit (12 messages total) correctly
reprocesses exactly **2**; a crash before the very first commit ever
happens (3 messages) correctly reprocesses all **3**. In every case,
the number reprocessed is never more than `COMMIT_EVERY - 1` — the
worst case is bounded and predictable, by design.

I also verified the timestamp-based replay's edge case: Kafka returns
offset `-1` when no message exists at or after the requested
timestamp (e.g., you ask for a moment in the future) — `replay_tool.py`
correctly falls back to `0` in that case instead of passing a broken
offset into `seek()`.

## 🏗️ Where this shows up for real

- **Any consumer behind a deploy/restart** — literally every consumer
  in a production system, all the time. This isn't a special case,
  it's the default operating condition.
- **Incident replay/backfill** — "our fraud detection logic had a bug
  for 20 minutes, replay that window through the fixed logic."
- **Disaster recovery drills** — proving a system can go down entirely
  and catch up cleanly, which teams actually rehearse on purpose.

## ▶️ Run it

1. Broker running (`docker compose up -d` from repo root).
2. Terminal 1 — start the steady event stream:
   ```bash
   python events_producer.py
   ```
3. Terminal 2 — the resilient consumer:
   ```bash
   python resilient_consumer.py
   ```
   Let it process a dozen or so events, then `Ctrl+C` it. Restart it —
   watch it resume from exactly the right offset, no gaps, no repeats.

4. Now simulate a real crash instead:
   ```bash
   python resilient_consumer.py --crash-after 12
   ```
   Watch it print which offset it last committed and how many messages
   will be reprocessed, then hard-exit. Run it again (same command,
   drop `--crash-after` this time) — watch it reprocess exactly the
   messages it warned you about, then continue normally.

5. Try the replay tool against whatever's accumulated in the topic:
   ```bash
   python replay_tool.py --from-beginning
   python replay_tool.py --from-offset 20
   python replay_tool.py --from-timestamp 1735000000   # use a real recent epoch time
   ```

## 👀 What to observe

- After a **graceful** `Ctrl+C`, restarting `resilient_consumer.py`
  processes only genuinely new events — nothing repeats.
- After `--crash-after N`, restarting it reprocesses a small number of
  already-seen events (up to 4, since commits happen every 5) — watch
  the `event_id`s printed on restart and confirm they match what the
  crash warning said would be reprocessed.
- `replay_tool.py --from-offset N` starts exactly at that offset,
  skipping everything before it — confirm the first `event_id` it
  prints matches what you'd expect from that offset.
- `replay_tool.py --from-timestamp` resolves to a specific offset
  before replaying anything — watch the printed "Resolved timestamp →
  starting offsets" line to see that resolution happen explicitly,
  not silently.
- All three tools use `TopicPartition` and Kafka Connect-free plain
  consumer APIs — nothing here needs Debezium, Kafka Streams, or any
  extra infrastructure beyond the broker itself.

## 🚀 Try breaking it

**1. Crash right on a commit boundary**

Try `--crash-after 10`, `--crash-after 15`, `--crash-after 20` (all
multiples of `COMMIT_EVERY=5`). Does it reprocess 0 messages every
time? Why would crashing exactly on a boundary be the "lucky" case?

**2. Lower `COMMIT_EVERY` to 1**

Change `COMMIT_EVERY = 5` to `COMMIT_EVERY = 1` in
`resilient_consumer.py`. Now a hard crash should never reprocess more
than... how many? What did you trade away to get that guarantee?

**3. Replay a window that doesn't exist yet**

Try `python replay_tool.py --from-timestamp 9999999999` (far in the
future). What happens — does it error, hang, or just replay nothing?
Does that match the `-1` fallback behavior discussed above?

## 💬 Questions worth answering yourself

- Why is "at most 4 messages reprocessed" a guarantee you can state
  confidently in advance, rather than something you'd have to test
  empirically every time?
- If `resilient_consumer.py` crashed twice in a row, before ever
  successfully committing once, what would restarting it a third time
  actually replay?
- `replay_tool.py` never commits any offsets under its own
  `group.id`. Why not? What would go wrong if it did?

This closes out the ten patterns from the infographic. Every one of
them, underneath the specific problem it solved, came back to the
same handful of primitives: a topic as a durable, ordered log; a
consumer group's offset as "where am I"; and the simple, load-bearing
fact that Kafka doesn't forget what's in the log just because nobody
was listening at the time.
