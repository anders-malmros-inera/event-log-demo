#!/bin/bash
# Stop Cassandra to demonstrate data persistence in Kafka

set -e

echo "[$(date)] Stopping Cassandra database..."
curl -X POST http://localhost:5000/control/cassandra/stop

echo "[$(date)] Cassandra stopped. Data remains in Kafka."
echo "[$(date)] Expected behavior: Producer and Kafka counts continue growing, Cassandra count stays static"
echo "[$(date)] Flink will buffer writes until Cassandra comes back"
