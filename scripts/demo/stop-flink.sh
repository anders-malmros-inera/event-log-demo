#!/bin/bash
# Stop Flink job manager to demonstrate queue buildup in Kafka

set -e

echo "[$(date)] Stopping Flink job manager..."
curl -X POST http://localhost:5000/control/flink/stop

echo "[$(date)] Flink stopped. Monitor Kafka lag at http://localhost:5000/monitor"
echo "[$(date)] Expected behavior: Kafka offsets will continue growing while Cassandra count stays static"
