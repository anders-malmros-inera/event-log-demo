#!/bin/bash
# Restart Flink to resume processing backlog

set -e

echo "[$(date)] Restarting Flink job manager..."
curl -X POST http://localhost:5000/control/flink/restart

echo "[$(date)] Flink restarted. It will now process the backlog."
echo "[$(date)] Expected behavior: Cassandra count will start increasing, catching up to Kafka offsets"
