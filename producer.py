"""
Kafka Producer
Project: Real-Time Traffic Incident Streaming System Using Apache Kafka

Reads simulated events from traffic_generator.py and publishes them to the
Kafka topic "traffic-events".

Prerequisites:
    1. Start ZooKeeper and the Kafka broker.
    2. Create the "traffic-events" topic if it does not already exist.
    3. Install the Kafka Python client package used by your environment.

Run:
    python producer.py
"""

import json
import random
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError

from traffic_generator import generate_traffic_event

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "traffic-events"


def create_producer():
    """Create a Kafka producer that serializes dictionaries as UTF-8 JSON."""
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
        acks=1,
        retries=3,
        retry_backoff_ms=500,
    )


def main():
    producer = None
    try:
        producer = create_producer()
        print(f"Kafka producer connected to {BOOTSTRAP_SERVERS}")
        print(f"Publishing traffic events to topic: {TOPIC_NAME}")
        print("Press Ctrl+C to stop.\n")

        while True:
            event = generate_traffic_event()
            future = producer.send(TOPIC_NAME, value=event)

            try:
                metadata = future.get(timeout=20)
                print(
                    "[PRODUCER] "
                    f"{event['incident_type']} -> {event['location']} -> "
                    f"{event['severity']} "
                    f"(topic={metadata.topic}, partition={metadata.partition}, "
                    f"offset={metadata.offset})"
                )
            except KafkaError as exc:
                print(f"[PRODUCER] Failed to publish event: {exc}")

            time.sleep(random.uniform(2.0, 3.0))

    except KeyboardInterrupt:
        print("\nStopping Kafka producer...")
    except Exception as exc:
        print(f"Could not start or run the Kafka producer: {exc}")
        print("Check that Kafka is running at 127.0.0.1:9092 and the topic exists.")
    finally:
        if producer is not None:
            producer.flush()
            producer.close()
        print("Kafka producer closed.")


if __name__ == "__main__":
    main()
