#!/bin/bash
# Restart Kafka to flush queued messages

set -e

echo "[$(date)] Restarting Kafka broker..."
curl -X POST http://localhost:5000/control/kafka/restart

echo "[$(date)] Kafka restarted. Producer will flush queued messages."
echo "[$(date)] Expected behavior: Kafka count will quickly catchup to producer count"
