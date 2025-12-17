#!/bin/bash
# Resume producer stream

set -e

echo "[$(date)] Resuming log producer stream..."
curl -X POST -H "Content-Type: application/json" -d '{"size_kb": 1, "rate": 10}' http://localhost:5000/start

echo "[$(date)] Producer stream started at 10 logs/sec, 1KB each"
echo "[$(date)] Expected behavior: All metrics resume growing"
