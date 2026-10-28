"""
Kafka Consumer
Project: Real-Time Traffic Incident Streaming System Using Apache Kafka

Consumes JSON traffic incident events from the "traffic-events" topic and
stores them in a local SQLite database named traffic.db.

Prerequisites:
    1. Start ZooKeeper and the Kafka broker.
    2. Start the Kafka producer.
    3. Install the Kafka Python client package used by your environment.

Run:
    python consumer.py
"""

import json
import sqlite3
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
TOPIC_NAME = "traffic-events"
GROUP_ID = "traffic-monitoring-group"
DATABASE_NAME = "traffic.db"


def initialize_database():
    """Create the incident table if it does not already exist."""
    with sqlite3.connect(DATABASE_NAME) as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS incidents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                incident_type TEXT NOT NULL,
                location TEXT NOT NULL,
                latitude REAL NOT NULL,
                longitude REAL NOT NULL,
                severity TEXT NOT NULL,
                vehicle_count INTEGER NOT NULL,
                average_speed REAL NOT NULL,
                timestamp TEXT NOT NULL,
                received_at TEXT NOT NULL
            )
            """
        )
        connection.commit()


def save_incident(event):
    """Validate required fields and save one event to SQLite."""
    required = (
        "incident_type",
        "location",
        "latitude",
        "longitude",
        "severity",
        "vehicle_count",
        "average_speed",
        "timestamp",
    )
    missing = [field for field in required if field not in event]
    if missing:
        raise ValueError(f"Missing required fields: {', '.join(missing)}")

    with sqlite3.connect(DATABASE_NAME) as connection:
        connection.execute(
            """
            INSERT INTO incidents (
                incident_type, location, latitude, longitude, severity,
                vehicle_count, average_speed, timestamp, received_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(event["incident_type"]),
                str(event["location"]),
                float(event["latitude"]),
                float(event["longitude"]),
                str(event["severity"]),
                int(event["vehicle_count"]),
                float(event["average_speed"]),
                str(event["timestamp"]),
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
            ),
        )
        connection.commit()


def main():
    initialize_database()
    consumer = None

    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
        )

        print(f"Kafka consumer listening on topic: {TOPIC_NAME}")
        print(f"Incident records will be stored in {DATABASE_NAME}")
        print("Press Ctrl+C to stop.\n")

        for message in consumer:
            try:
                event = message.value
                save_incident(event)
                print(
                    f"[CONSUMER] Saved {event.get('incident_type', 'Unknown')} "
                    f"-> {event.get('location', 'Unknown')} "
                    f"(database ID assigned)"
                )
            except (ValueError, TypeError, KeyError, sqlite3.Error, json.JSONDecodeError) as exc:
                print(f"[CONSUMER] Skipped invalid event: {exc}")

    except KeyboardInterrupt:
        print("\nStopping Kafka consumer...")
    except (KafkaError, Exception) as exc:
        print(f"Consumer could not run: {exc}")
        print("Check Kafka at 127.0.0.1:9092 and verify the topic exists.")
    finally:
        if consumer is not None:
            consumer.close()
        print("Kafka consumer closed.")


if __name__ == "__main__":
    main()
