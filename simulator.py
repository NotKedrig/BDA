import csv
import json
import os
import random
import time
from typing import Dict, Iterator

from kafka import KafkaProducer


DATA_PATH = os.path.join("archive", "deliveries.csv")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
TOPIC_NAME = "ipl_match_stream"

# Exciting match to stream (2nd innings only)
MATCH_ID = 335984
TARGET_INNING = 2


def read_match_deliveries(path: str, match_id: int, inning: int) -> Iterator[Dict]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                if int(row["match_id"]) == match_id and int(row["inning"]) == inning:
                    yield row
            except (KeyError, ValueError):
                continue


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: str(v).encode("utf-8") if v is not None else None,
    )


def main() -> None:
    print(f"Connecting to Kafka at {KAFKA_BROKER} ...")
    producer = create_producer()
    print(f"Streaming match_id={MATCH_ID}, inning={TARGET_INNING} to topic '{TOPIC_NAME}'")

    for delivery in read_match_deliveries(DATA_PATH, MATCH_ID, TARGET_INNING):
        key = f"{delivery['match_id']}_{delivery['inning']}_{delivery['over']}_{delivery['ball']}"
        producer.send(TOPIC_NAME, key=key, value=delivery)
        print(f"Sent: {key}")
        # Small random delay to mimic live feed
        time.sleep(random.uniform(0.5, 1.0))

    producer.flush()
    print("Finished streaming all deliveries.")


if __name__ == "__main__":
    main()

