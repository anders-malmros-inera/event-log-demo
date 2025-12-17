#!/bin/bash
# Stop Kafka broker to demonstrate producer-side queueing

set -e

echo "[$(date)] Stopping Kafka broker..."
curl -X POST http://localhost:5000/control/kafka/stop

echo "[$(date)] Kafka stopped. Producer will queue messages in memory."
echo "[$(date)] Expected behavior: Producer count increases, Kafka/Cassandra counts remain static"
echo "[$(date)] Check metrics: http://localhost:5000/monitor"
