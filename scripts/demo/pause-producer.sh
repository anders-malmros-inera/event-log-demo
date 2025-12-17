#!/bin/bash
# Pause producer stream to demonstrate flow halt

set -e

echo "[$(date)] Pausing log producer stream..."
curl -X POST http://localhost:5000/stop

echo "[$(date)] Producer stream stopped. All metrics should remain static."
echo "[$(date)] Expected behavior: Producer, Kafka, and Cassandra counts stop changing"
echo "[$(date)] Check metrics: http://localhost:5000/monitor"
