"""
Pattern 7: Event Sourcing -- topic setup
--------------------------------------------
Creates account-events with multiple partitions. This is safe for
event sourcing here because every event is keyed by account_id --
Kafka guarantees all of one account's events land in the same
partition, in order, no matter how many partitions the topic has or
how other accounts' events interleave around them. rebuild_balance.py
already reads across every partition when replaying, so this works
without any code changes.

Run this ONCE before starting the producer:
    python create_topic.py
"""

from confluent_kafka.admin import AdminClient, NewTopic

TOPIC = "account-events"
NUM_PARTITIONS = 3


def main():
    admin = AdminClient({"bootstrap.servers": "localhost:9092"})

    existing = admin.list_topics(timeout=5).topics
    if TOPIC in existing:
        print(f"Topic '{TOPIC}' already exists with "
              f"{len(existing[TOPIC].partitions)} partition(s). "
              f"Delete it first if you want to recreate with {NUM_PARTITIONS}.")
        return

    new_topic = NewTopic(TOPIC, num_partitions=NUM_PARTITIONS, replication_factor=1)
    futures = admin.create_topics([new_topic])

    for topic, future in futures.items():
        future.result()
        print(f"Created topic '{topic}' with {NUM_PARTITIONS} partitions.")


if __name__ == "__main__":
    main()