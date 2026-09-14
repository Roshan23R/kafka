"""
Pattern 10: Replay & Recovery
--------------------------------
A standalone debugging/recovery tool: replay events starting from ANY
point you choose -- not just "the beginning" (Pattern 7 always started
from offset 0). This is what you'd reach for after an incident: "our
downstream system had a bug between 2:14pm and 2:20pm, replay exactly
that window so we can reprocess it correctly."

Three ways to choose where to start:
    --from-beginning        start at offset 0 (same as Pattern 7)
    --from-offset N         start at a specific, known offset
    --from-timestamp EPOCH  start at the first event at/after this
                             Unix timestamp (seconds) -- Kafka can look
                             this up directly, no manual scanning needed

Always replays forward to "the end as of right now" and then stops --
same one-shot, watermark-based approach as Pattern 7's rebuild script.

Run:
    python replay_tool.py --from-beginning
    python replay_tool.py --from-offset 20
    python replay_tool.py --from-timestamp 1735000000
"""

import json
import argparse
import time
from confluent_kafka import Consumer, TopicPartition

TOPIC = "recovery-events"


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--from-beginning", action="store_true", help="Replay from offset 0")
    group.add_argument("--from-offset", type=int, help="Replay from this exact offset")
    group.add_argument("--from-timestamp", type=float, help="Replay from the first event at/after this Unix timestamp")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "replay-tool-oneshot",  # throwaway -- see Pattern 7 for why
    })

    metadata = consumer.list_topics(TOPIC, timeout=5)
    partitions = list(metadata.topics[TOPIC].partitions.keys())
    topic_partitions = [TopicPartition(TOPIC, p) for p in partitions]
    consumer.assign(topic_partitions)

    # figure out where to START on each partition
    if args.from_timestamp is not None:
        # Kafka can look up "the offset of the first message at or after
        # this timestamp" directly -- no need to scan from 0 yourself
        ts_ms = int(args.from_timestamp * 1000)
        search = [TopicPartition(TOPIC, p, ts_ms) for p in partitions]
        results = consumer.offsets_for_times(search, timeout=5)
        start_offsets = {tp.partition: (tp.offset if tp.offset >= 0 else 0) for tp in results}
        print(f"Resolved timestamp {args.from_timestamp} -> starting offsets: {start_offsets}")
    elif args.from_offset is not None:
        start_offsets = {p: args.from_offset for p in partitions}
    else:
        start_offsets = {p: 0 for p in partitions}

    # figure out where to STOP: "the end, as of right now" (same
    # watermark technique as Pattern 7 -- a fixed, known finish line)
    end_offsets = {}
    for p in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(TOPIC, p), timeout=5)
        end_offsets[p] = high
        consumer.seek(TopicPartition(TOPIC, p, start_offsets[p]))

    print(f"Replaying '{TOPIC}' from {start_offsets} to {end_offsets}...")

    current_offsets = dict(start_offsets)

    def caught_up():
        return all(current_offsets[p] >= end_offsets[p] for p in partitions)

    count = 0
    while not caught_up():
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        current_offsets[msg.partition()] = msg.offset() + 1
        event = json.loads(msg.value())
        count += 1
        print(f"  replay -> event {event['event_id']} (offset {msg.offset()}, "
              f"produced at {time.strftime('%H:%M:%S', time.localtime(event['ts']))})")

    consumer.close()
    print(f"\nReplayed {count} event(s).")


if __name__ == "__main__":
    main()
