"""
Pattern 10: Replay & Recovery
--------------------------------
A consumer that commits its offset MANUALLY, every COMMIT_EVERY
messages (not after every single one -- committing every message is
safest but slowest; committing periodically is the common real-world
tradeoff). This makes the "how much gets reprocessed after a crash"
question concrete instead of theoretical.

Two ways to stop this script, on purpose, to show two different
outcomes:

  Ctrl+C (graceful)   -> the finally block commits whatever's left,
                         so a clean restart reprocesses NOTHING.

  --crash-after N     -> after processing exactly N messages, this
                         calls os._exit() -- a hard kill that skips
                         the finally block entirely, exactly like a
                         real crash (OOM kill, power loss, segfault).
                         Whatever was processed since the LAST commit
                         is NOT saved, and gets reprocessed on the
                         next run. Nothing is ever silently lost --
                         at worst, something gets seen twice.

Run:
    python resilient_consumer.py
    python resilient_consumer.py --crash-after 12
"""

import os
import json
import argparse
from confluent_kafka import Consumer

TOPIC = "recovery-events"
COMMIT_EVERY = 5  # commit every 5 processed messages, not every single one


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--crash-after", type=int, default=None,
                         help="Hard-exit (simulating a real crash) after processing this many messages")
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "resilient-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # we commit manually, on our own schedule
        }
    )
    consumer.subscribe([TOPIC])

    print(f"Consuming '{TOPIC}', committing every {COMMIT_EVERY} messages "
          f"(Ctrl+C to stop cleanly{', or crashing after ' + str(args.crash_after) + ' messages' if args.crash_after else ''})...")

    processed_since_commit = 0
    total_processed = 0
    last_msg = None

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            event = json.loads(msg.value())
            total_processed += 1
            processed_since_commit += 1
            last_msg = msg
            print(f"  processed event {event['event_id']} (offset {msg.offset()}, "
                  f"{processed_since_commit}/{COMMIT_EVERY} since last commit)")

            if processed_since_commit >= COMMIT_EVERY:
                consumer.commit(msg)
                print(f"  -- committed up through offset {msg.offset()} --")
                processed_since_commit = 0

            if args.crash_after and total_processed >= args.crash_after:
                print(f"\n!!! SIMULATING A HARD CRASH after {total_processed} messages !!!")
                print(f"!!! {processed_since_commit} messages processed since last commit "
                      f"will be REPROCESSED on next run !!!")
                os._exit(1)  # skips the finally block entirely -- a real, unclean crash

    except KeyboardInterrupt:
        pass
    finally:
        # graceful path only -- never reached after os._exit()
        if last_msg is not None and processed_since_commit > 0:
            consumer.commit(last_msg)
            print(f"Clean shutdown: committed remaining {processed_since_commit} message(s).")
        consumer.close()
        print(f"Total processed this run: {total_processed}")


if __name__ == "__main__":
    main()
