"""
Synthetic Traffic Event Generator
Project: Real-Time Traffic Incident Streaming System Using Apache Kafka

This script generates simulated traffic incident records for selected Hyderabad
locations and prints each event as JSON. It can be used independently to test
the event format before connecting it to the Kafka producer.

Run:
    python traffic_generator.py

Note:
    This is a starter implementation for the project repository. It generates
    simulated data and does not collect data from real traffic sensors.
"""

import json
import random
import time
from datetime import datetime

# Approximate base coordinates for selected Hyderabad locations.
LOCATIONS = {
    "Ameerpet": (17.4375, 78.4483),
    "Punjagutta": (17.4239, 78.4483),
    "Begumpet": (17.4447, 78.4660),
    "Banjara Hills": (17.4126, 78.4482),
    "Hitech City": (17.4435, 78.3772),
    "Charminar": (17.3616, 78.4747),
    "Madhapur": (17.4483, 78.3915),
    "Kukatpally": (17.4849, 78.4138),
    "Secunderabad": (17.4399, 78.4983),
    "LB Nagar": (17.3457, 78.5522),
    "Mehdipatnam": (17.3950, 78.4360),
    "Kondapur": (17.4604, 78.3568),
}

# Profiles define severity and approximate traffic measurements.
INCIDENT_PROFILES = {
    "Accident": {
        "weight": 0.25,
        "severities": ["High", "Medium"],
        "speed_range": (0, 20),
        "vehicle_range": (2, 25),
    },
    "Heavy Congestion": {
        "weight": 0.35,
        "severities": ["Medium", "High"],
        "speed_range": (2, 18),
        "vehicle_range": (10, 80),
    },
    "Road Block": {
        "weight": 0.15,
        "severities": ["High", "Medium"],
        "speed_range": (0, 10),
        "vehicle_range": (5, 50),
    },
    "Normal": {
        "weight": 0.25,
        "severities": ["Low"],
        "speed_range": (25, 60),
        "vehicle_range": (1, 30),
    },
}


def generate_traffic_event():
    """Create and return one simulated traffic incident as a dictionary."""
    location = random.choice(list(LOCATIONS.keys()))
    base_latitude, base_longitude = LOCATIONS[location]

    # Add a small random offset so repeated events do not share identical points.
    latitude = base_latitude + random.uniform(-0.003, 0.003)
    longitude = base_longitude + random.uniform(-0.003, 0.003)

    incident_type = random.choices(
        population=list(INCIDENT_PROFILES.keys()),
        weights=[profile["weight"] for profile in INCIDENT_PROFILES.values()],
        k=1,
    )[0]
    profile = INCIDENT_PROFILES[incident_type]

    return {
        "incident_type": incident_type,
        "location": location,
        "latitude": round(latitude, 6),
        "longitude": round(longitude, 6),
        "severity": random.choice(profile["severities"]),
        "vehicle_count": random.randint(*profile["vehicle_range"]),
        "average_speed": round(random.uniform(*profile["speed_range"]), 1),
        "timestamp": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S"),
    }


def main():
    """Print simulated JSON events continuously until interrupted."""
    print("Starting simulated Hyderabad traffic event generator.")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            event = generate_traffic_event()
            print(json.dumps(event, ensure_ascii=False))
            time.sleep(random.uniform(2.0, 3.0))
    except KeyboardInterrupt:
        print("\nTraffic event generator stopped.")


if __name__ == "__main__":
    main()
