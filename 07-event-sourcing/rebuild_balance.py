"""
Pattern 7: Event Sourcing
----------------------------
Rebuilds account balances from SCRATCH by replaying the entire
account-events log from the very beginning, folding each DEPOSIT/
WITHDRAW into a running total. This is what "state as an append-only
log" means in practice: balance isn't read from storage, it's
COMPUTED by re-processing every event that ever happened, in order.

Unlike other consumers in this repo, this one deliberately reads to
the END of the topic and then stops -- it's a one-shot batch replay,
not a continuously running service. It uses manual partition
assignment + watermark offsets to know exactly when it has caught up,
rather than relying on a consumer group + guessing via a timeout.

Run:
    python rebuild_balance.py
    python rebuild_balance.py --account acc-1
    python rebuild_balance.py --verbose   # show every event as it replays
"""

import json
import argparse
from confluent_kafka import Consumer, TopicPartition

TOPIC = "account-events"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", help="Only show this account's balance")
    parser.add_argument("--verbose", action="store_true", help="Print every event while replaying")
    args = parser.parse_args()

    # a throwaway group.id -- we're not tracking offsets across runs here,
    # every run replays the FULL log from the beginning on purpose
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "rebuild-balance-oneshot",
        }
    )

    # manual partition assignment: find every partition of the topic and
    # figure out exactly where "the end" currently is (the high watermark),
    # so we know precisely when replay is complete instead of guessing
    metadata = consumer.list_topics(TOPIC, timeout=5)
    partitions = list(metadata.topics[TOPIC].partitions.keys())

    topic_partitions = [TopicPartition(TOPIC, p) for p in partitions]
    consumer.assign(topic_partitions)

    end_offsets = {}
    for tp in topic_partitions:
        low, high = consumer.get_watermark_offsets(tp, timeout=5)
        end_offsets[tp.partition] = high
        consumer.seek(TopicPartition(TOPIC, tp.partition, 0))  # start from offset 0 -- the beginning

    print(f"Replaying '{TOPIC}' from the beginning to rebuild balances...")

    balances = {}
    current_offsets = {p: 0 for p in partitions}

    def caught_up():
        return all(current_offsets[p] >= end_offsets[p] for p in partitions)

    while not caught_up():
        msg = consumer.poll(1.0)
        if msg is None:
            break  # nothing left to read
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        current_offsets[msg.partition()] = msg.offset() + 1
        event = json.loads(msg.value())
        acc = event["account_id"]

        delta = event["amount"] if event["event_type"] == "DEPOSIT" else -event["amount"]
        balances[acc] = round(balances.get(acc, 0) + delta, 2)

        if args.verbose:
            sign = "+" if delta >= 0 else ""
            print(f"  replay -> {acc}: {event['event_type']} {sign}{delta}  (running balance: {balances[acc]})")

    consumer.close()

    print("\n=== Rebuilt balances (from replaying the full log) ===")
    if args.account:
        bal = balances.get(args.account, 0)
        print(f"{args.account}: {bal}")
    else:
        for acc, bal in sorted(balances.items()):
            print(f"{acc}: {bal}")


if __name__ == "__main__":
    main()