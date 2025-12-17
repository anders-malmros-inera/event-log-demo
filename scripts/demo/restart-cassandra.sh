#!/bin/bash
# Restart Cassandra to resume data persistence

set -e

echo "[$(date)] Restarting Cassandra database..."
curl -X POST http://localhost:5000/control/cassandra/restart

echo "[$(date)] Cassandra restarted. Flink will flush buffered writes."
echo "[$(date)] Expected behavior: Cassandra count will increase to match Kafka offsets"
