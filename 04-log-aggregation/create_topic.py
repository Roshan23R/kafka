"""
Pattern 4: Log Aggregation -- topic setup
--------------------------------------------
By default, auto-created topics on this broker get 1 partition -- with
only one partition, there's nowhere else for a message to go, so you
can't actually observe key-based routing. This script explicitly
creates the "logs" topic with 3 partitions so the routing claim in the
README can be tested for real.

Run this ONCE before starting the producers/aggregator:
    python create_topic.py
"""

from confluent_kafka.admin import AdminClient, NewTopic

TOPIC = "logs"
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
        future.result()  # raises if creation failed
        print(f"Created topic '{topic}' with {NUM_PARTITIONS} partitions.")


if __name__ == "__main__":
    main()