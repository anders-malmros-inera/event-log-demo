# Acceptance Testing Guide

## Overview

Acceptance tests verify end-to-end behavior of failure scenarios. These tests:
- Use real HTTP calls to producer API
- Control services via `/control` endpoints
- Verify metrics reflect expected state changes
- Test actual data flow through the pipeline

## Running Acceptance Tests

**Prerequisites**: All services must be running.

```bash
# Start the full stack
docker compose up -d

# Wait for services to initialize (~2 minutes)
docker compose ps

# Run acceptance tests
docker compose --profile acceptance run --rm acceptance-tests
```

## Test Scenarios

### test_stop_producer_stops_data_flow
**Verifies**: When producer stops, all metrics remain static.

### test_stop_kafka_causes_producer_buffering
**Verifies**: When Kafka stops, producer continues but Kafka metrics stay static.

### test_restart_kafka_resumes_data_flow
**Verifies**: After Kafka restart, metrics resume increasing.

### test_stop_flink_causes_kafka_lag
**Verifies**: When Flink stops, Kafka continues but Cassandra stays static (lag grows).

### test_metrics_show_errors_when_services_unavailable
**Verifies**: Unavailable services show -1 in metrics.

### test_control_api_validates_service_names
**Verifies**: Invalid service names return 400 error.

### test_control_api_validates_actions
**Verifies**: Invalid actions return 400 error.

## Test Execution Notes

**Timing:**
- Tests wait 12-15 seconds between operations for metrics to update
- Background scheduler refreshes every 10 seconds
- Some tests may take 30-60 seconds to complete

**Service Dependencies:**
- Tests may restart services during execution
- Kafka takes 5-10 seconds to fully initialize after restart
- Flink startup is faster (~3-5 seconds)

**Known Limitations:**
- Tests assume services start successfully (may show -1 during initialization)
- Network timing may cause occasional flakiness
- Tests are sequential (one at a time) to avoid interference

## Manual Verification

If automated tests are unreliable, verify manually:

1. **Start producer**:
   ```bash
   curl -X POST -H "Content-Type: application/json" \
     -d '{"rate": 5, "size_kb": 1}' http://localhost:5000/start
   ```

2. **Get baseline**:
   ```bash
   curl http://localhost:5000/monitor
   ```

3. **Stop Kafka**:
   ```bash
   curl -X POST http://localhost:5000/control/kafka/stop
   ```

4. **Wait 15 seconds and check**:
   ```bash
   curl http://localhost:5000/monitor
   # Kafka count should be static or -1
   ```

5. **Restart Kafka**:
   ```bash
   curl -X POST http://localhost:5000/control/kafka/start
   ```

6. **Wait 15 seconds and verify recovery**:
   ```bash
   curl http://localhost:5000/monitor
   # Kafka count should resume increasing
   ```

## Debugging Failed Tests

**Check producer logs**:
```bash
docker compose logs producer --tail=50
```

**Check Kafka status**:
```bash
docker compose ps kafka
docker compose logs kafka --tail=20
```

**Verify control API**:
```bash
curl http://localhost:5000/control/kafka/status
```

**Reset state**:
```bash
docker compose restart producer
```
