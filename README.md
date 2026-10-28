# Real-Time Traffic Incident Streaming System Using Apache Kafka

## 1. Project Overview

Modern metropolitan transport networks face severe congestion and unpredictable traffic incidents. Delays in detecting and communicating incidents can increase traffic congestion and affect emergency response.

This project implements a lightweight, event-driven traffic incident streaming system using Apache Kafka. It simulates traffic incidents at different locations in Hyderabad, streams the generated events through Kafka, stores incident information in SQLite, and presents the data through a web-based dashboard.

## 2. Problem Statement

Traditional traffic monitoring systems may depend on batch processing and tightly coupled request-response communication. These approaches can introduce delays, limit scalability, and make it difficult to handle continuous traffic telemetry.

The proposed system uses Apache Kafka to provide asynchronous event streaming and separates event generation, message transmission, storage, and visualization.

## 3. Project Objectives

- Generate simulated traffic incidents using Python.
- Publish traffic events as JSON messages to Apache Kafka.
- Consume and validate events from the Kafka topic.
- Store incident records in an SQLite database.
- Display traffic incidents using an interactive geospatial dashboard.
- Present alerts, incident statistics, and traffic analytics.

## 4. Technologies Used

- **Programming Language:** Python
- **Streaming Platform:** Apache Kafka 3.6.0
- **Coordination:** Apache ZooKeeper
- **Backend Framework:** Flask
- **Database:** SQLite
- **Frontend:** HTML5, CSS3, and JavaScript
- **Mapping Library:** Leaflet.js
- **Map Data:** OpenStreetMap
- **Analytics Charts:** Chart.js

## 5. System Architecture

The system follows an event-driven publish-subscribe architecture.

Traffic Data Generator → Kafka Producer → Kafka Broker → Kafka Consumer → SQLite Database → Flask Web Dashboard

### Main Components

1. **Traffic Data Generator:** Simulates traffic incidents across selected Hyderabad locations.
2. **Kafka Producer:** Converts incident information into JSON messages and publishes them to Kafka.
3. **Kafka Broker:** Receives and stores messages in the `traffic-events` topic.
4. **Kafka Consumer:** Reads and processes incoming traffic events.
5. **SQLite Database:** Stores incident records for retrieval and analysis.
6. **Flask Dashboard:** Provides APIs, incident visualization, alerts, and statistics.

## 6. Traffic Event Information

Each simulated incident can contain the following information:

- Incident type
- Location
- Latitude and longitude
- Severity
- Vehicle count
- Average speed
- Timestamp

Incident categories include accidents, heavy congestion, road blocks, and normal traffic.

## 7. Dashboard Features

- Live incident statistics
- Interactive Hyderabad traffic map
- Color-coded incident markers
- High-severity alert banner
- Live event feed
- Historical incident log
- Traffic analytics charts
- Average city speed and critical incident ratio

## 8. Expected Results

The system is designed to demonstrate continuous traffic event generation, asynchronous Kafka message delivery, database persistence, and live dashboard updates.

The project report describes an overall visual update latency of approximately 0.05–2.05 seconds, with a mean of approximately 1.05 seconds under the reported experimental setup.

## 9. Advantages

- Near-real-time traffic incident visualization
- Decoupled producer and consumer components
- Hardware-free traffic simulation
- Persistent event storage through Kafka
- Historical incident storage in SQLite
- Scope for adding analytics and additional consumers

## 10. Limitations

- Traffic information is simulated rather than collected from real sensors.
- The prototype uses a single local Kafka broker.
- Automated camera-based accident detection is not implemented.
- The dashboard uses periodic AJAX polling.
- The local setup requires Java, Kafka, ZooKeeper, and appropriate port configuration.

## 11. Future Scope

- Integrate camera-based incident detection using YOLOv8.
- Add real-time stream processing using Kafka Streams or Apache Flink.
- Develop congestion forecasting using machine-learning models.
- Replace periodic polling with WebSockets or Server-Sent Events.
- Integrate emergency services and develop mobile traffic alerts.

## 12. Project Documentation

The repository should include the verified project report and the actual source code used for implementation.

## 13. Author

**Student Project:** Real-Time Traffic Incident Streaming System Using Apache Kafka

**Repository:** https://github.com/NamdeSham28/traffic-kafka-project
