"""
producer.py
────────────
Reads amazon_user_activity_streaming_events.csv row-by-row and publishes
each event as a JSON message to a Kafka topic, simulating a live event stream.

Environment variables:
  KAFKA_BOOTSTRAP_SERVERS  default: kafka:9092
  KAFKA_TOPIC              default: user_events
  DATA_FILE                default: /project_data/amazon_user_activity_streaming_events.csv
  DELAY_MS                 ms to sleep between messages (default: 10)
  LOOP                     if "true", replay the file indefinitely (default: false)
"""

import csv
import json
import os
import time
from datetime import datetime, timezone

from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC             = os.getenv("KAFKA_TOPIC", "user_events")
DATA_FILE         = os.getenv("DATA_FILE", "/project_data/amazon_user_activity_streaming_events.csv")
DELAY_MS          = float(os.getenv("DELAY_MS", "10"))
LOOP              = os.getenv("LOOP", "false").lower() == "true"


def wait_for_kafka(timeout: int = 120):
    print("  Waiting for Kafka broker...", flush=True)
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            p = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
            p.close()
            print("  ✓ Kafka broker ready", flush=True)
            return
        except NoBrokersAvailable:
            time.sleep(3)
    raise RuntimeError(f"Kafka not reachable after {timeout}s")


def ensure_topic():
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        admin.create_topics([
            NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)
        ])
        print(f"  ✓ Topic '{TOPIC}' created (3 partitions)", flush=True)
    except TopicAlreadyExistsError:
        print(f"  ✓ Topic '{TOPIC}' already exists", flush=True)
    finally:
        admin.close()


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        linger_ms=5,          # micro-batch sends for throughput
        batch_size=16_384,
    )


def stream_file(producer: KafkaProducer) -> int:
    sent = 0
    delay_s = DELAY_MS / 1000.0

    with open(DATA_FILE, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            row["produced_at"] = datetime.now(timezone.utc).isoformat()
            producer.send(TOPIC, key=row["event_id"], value=dict(row))
            sent += 1

            if sent % 1000 == 0:
                producer.flush()
                print(f"  → {sent:,} events sent", flush=True)

            if delay_s > 0:
                time.sleep(delay_s)

    producer.flush()
    return sent


def main():
    sep = "=" * 50
    print(f"\n{sep}")
    print("  Kafka User Events Producer")
    print(f"{sep}")
    print(f"  Broker : {BOOTSTRAP_SERVERS}")
    print(f"  Topic  : {TOPIC}")
    print(f"  File   : {DATA_FILE}")
    print(f"  Delay  : {DELAY_MS} ms/event")
    print(f"  Loop   : {LOOP}\n")

    wait_for_kafka()
    ensure_topic()

    producer = make_producer()
    pass_num = 0

    try:
        while True:
            pass_num += 1
            print(f"\n  Pass {pass_num} — streaming events...", flush=True)
            total = stream_file(producer)
            print(f"  ✓ Pass {pass_num} complete — {total:,} events sent to '{TOPIC}'")
            if not LOOP:
                break
    finally:
        producer.close()

    print(f"\n{sep}")
    print("  ✓ Producer finished")
    print(f"{sep}\n")


if __name__ == "__main__":
    main()
