"""
Flask Web Application
Project: Real-Time Traffic Incident Streaming System Using Apache Kafka

Provides a small dashboard and REST endpoints backed by the SQLite database
created by consumer.py.

Run:
    python app.py

Then open:
    http://127.0.0.1:5000

Prerequisites:
    pip install Flask
    Run consumer.py at least once so the database/table is initialized.
"""

import sqlite3
from pathlib import Path

from flask import Flask, jsonify, render_template

app = Flask(__name__)
DATABASE_NAME = Path(__file__).with_name("traffic.db")


def get_connection():
    """Return a SQLite connection with dictionary-like rows."""
    connection = sqlite3.connect(DATABASE_NAME)
    connection.row_factory = sqlite3.Row
    return connection


def fetch_incidents(limit=100):
    """Return newest incidents first."""
    if not DATABASE_NAME.exists():
        return []

    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT id, incident_type, location, latitude, longitude,
                   severity, vehicle_count, average_speed, timestamp
            FROM incidents
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


@app.route("/")
def index():
    """Render the dashboard template."""
    return render_template("index.html")


@app.route("/api/incidents")
def api_incidents():
    """Return recent incidents as JSON."""
    return jsonify(fetch_incidents())


@app.route("/api/stats")
def api_stats():
    """Return basic traffic statistics as JSON."""
    if not DATABASE_NAME.exists():
        return jsonify({
            "total_events": 0,
            "active_incidents": 0,
            "high_severity": 0,
            "average_speed": 0,
            "incident_types": {},
        })

    with get_connection() as connection:
        total = connection.execute(
            "SELECT COUNT(*) FROM incidents"
        ).fetchone()[0]
        high = connection.execute(
            "SELECT COUNT(*) FROM incidents WHERE severity = 'High'"
        ).fetchone()[0]
        average_speed = connection.execute(
            "SELECT AVG(average_speed) FROM incidents"
        ).fetchone()[0]
        type_rows = connection.execute(
            """
            SELECT incident_type, COUNT(*) AS count
            FROM incidents
            GROUP BY incident_type
            """
        ).fetchall()

    incident_types = {row["incident_type"]: row["count"] for row in type_rows}
    return jsonify({
        "total_events": total,
        "active_incidents": total,
        "high_severity": high,
        "average_speed": round(average_speed or 0, 1),
        "incident_types": incident_types,
    })


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
