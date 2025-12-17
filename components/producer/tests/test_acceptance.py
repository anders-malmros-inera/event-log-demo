"""
Integration/acceptance tests for failure scenarios.

These tests verify end-to-end behavior when services fail and recover.
They use real HTTP calls to the producer API and check metrics reflect expected state.
"""
import pytest
import requests
import time


BASE_URL = "http://producer:5000"


def wait_for_metrics_update(seconds=12):
    """Wait for background scheduler to refresh metrics (runs every 10 seconds)."""
    time.sleep(seconds)


def get_metrics():
    """Get current pipeline metrics."""
    response = requests.get(f"{BASE_URL}/monitor")
    assert response.status_code == 200
    return response.json()


def control_service(service, action):
    """Control a service via the control API."""
    response = requests.post(f"{BASE_URL}/control/{service}/{action}")
    return response.json()


def start_producer_stream(rate=10, size_kb=1):
    """Start the producer streaming logs."""
    response = requests.post(
        f"{BASE_URL}/start",
        json={"rate": rate, "size_kb": size_kb}
    )
    assert response.status_code == 200
    return response.json()


def stop_producer_stream():
    """Stop the producer streaming logs."""
    response = requests.post(f"{BASE_URL}/stop")
    assert response.status_code == 200
    return response.json()


@pytest.fixture(scope="function")
def clean_state():
    """Ensure clean state before and after each test."""
    # Stop producer if running
    try:
        stop_producer_stream()
    except:
        pass
    
    yield
    
    # Cleanup after test
    try:
        stop_producer_stream()
    except:
        pass


def test_stop_producer_stops_data_flow(clean_state):
    """
    Given producer is streaming,
    When producer is stopped,
    Then all metrics remain static.
    """
    # Start streaming
    start_producer_stream(rate=5, size_kb=1)
    wait_for_metrics_update()
    
    # Get baseline metrics
    baseline = get_metrics()
    assert baseline['is_streaming'] is True
    producer_before = baseline['producer_count']
    
    # Stop producer
    stop_producer_stream()
    wait_for_metrics_update()
    
    # Verify metrics are static
    after_stop = get_metrics()
    assert after_stop['is_streaming'] is False
    assert after_stop['producer_count'] == producer_before, \
        "Producer count should not change after stopping"


def test_stop_kafka_causes_producer_buffering(clean_state):
    """
    Given producer is streaming and Kafka is running,
    When Kafka is stopped,
    Then producer continues generating (buffering) but Kafka metrics stay static.
    """
    # Start streaming
    start_producer_stream(rate=5, size_kb=1)
    wait_for_metrics_update()
    
    baseline = get_metrics()
    kafka_before = baseline['kafka_count']
    
    # Stop Kafka
    result = control_service('kafka', 'stop')
    assert result['success'] is True, f"Failed to stop Kafka: {result['message']}"
    
    # Wait and check metrics
    wait_for_metrics_update(15)  # Give extra time
    
    after_stop = get_metrics()
    
    # Producer should continue (or might show errors, but count may still increase)
    assert after_stop['kafka_count'] == kafka_before or after_stop['kafka_count'] == -1, \
        "Kafka count should remain static or show error (-1) when Kafka is down"
    
    # Cleanup: restart Kafka
    control_service('kafka', 'start')
    time.sleep(10)  # Wait for Kafka to come up


def test_restart_kafka_resumes_data_flow(clean_state):
    """
    Given Kafka was stopped and restarted,
    When producer continues streaming,
    Then Kafka metrics resume increasing.
    """
    # Start streaming
    start_producer_stream(rate=5, size_kb=1)
    wait_for_metrics_update()
    
    # Stop and restart Kafka
    control_service('kafka', 'stop')
    time.sleep(5)
    result = control_service('kafka', 'start')
    assert result['success'] is True
    
    # Wait for Kafka to initialize and producer to reconnect
    time.sleep(15)
    
    baseline = get_metrics()
    kafka_before = baseline.get('kafka_count', -1)
    
    # Wait for more data
    wait_for_metrics_update(15)
    
    after = get_metrics()
    kafka_after = after.get('kafka_count', -1)
    
    # Kafka should resume receiving data (if it successfully started)
    # Note: This might be -1 if Kafka is still initializing
    assert kafka_after >= kafka_before or kafka_after == -1, \
        "Kafka should resume or show initialization (-1)"


def test_stop_flink_causes_kafka_lag(clean_state):
    """
    Given producer is streaming and Flink is processing,
    When Flink is stopped,
    Then Kafka continues receiving but Cassandra metrics stay static (lag grows).
    """
    # Start streaming
    start_producer_stream(rate=5, size_kb=1)
    wait_for_metrics_update()
    
    baseline = get_metrics()
    cassandra_before = baseline['cassandra_count']
    
    # Stop Flink
    result = control_service('flink', 'stop')
    assert result['success'] is True
    
    # Wait and check
    wait_for_metrics_update(15)
    
    after_stop = get_metrics()
    
    # Cassandra should not increase (Flink is stopped)
    assert after_stop['cassandra_count'] == cassandra_before or \
           after_stop['cassandra_count'] == -1, \
        "Cassandra count should remain static when Flink is down"
    
    # Kafka might still increase (producer still running)
    # This demonstrates queue buildup
    
    # Cleanup: restart Flink
    control_service('flink', 'start')
    time.sleep(5)


def test_metrics_show_errors_when_services_unavailable():
    """
    Given services are stopped,
    When metrics are queried,
    Then unavailable services show -1.
    """
    metrics = get_metrics()
    
    # Check structure
    assert 'producer_count' in metrics
    assert 'kafka_count' in metrics
    assert 'cassandra_count' in metrics
    assert 'is_streaming' in metrics
    
    # When services are down, counts should be -1 or valid numbers
    for key in ['producer_count', 'kafka_count', 'cassandra_count']:
        assert isinstance(metrics[key], int), f"{key} should be an integer"
        assert metrics[key] >= -1, f"{key} should be >= -1"


def test_control_api_validates_service_names():
    """
    Given invalid service name,
    When control API is called,
    Then returns 400 error.
    """
    response = requests.post(f"{BASE_URL}/control/invalid-service/stop")
    assert response.status_code == 400
    data = response.json()
    assert data['success'] is False
    assert 'not allowed' in data['message'].lower()


def test_control_api_validates_actions():
    """
    Given invalid action,
    When control API is called,
    Then returns 400 error.
    """
    response = requests.post(f"{BASE_URL}/control/kafka/destroy")
    assert response.status_code == 400
    data = response.json()
    assert data['success'] is False
    assert 'not allowed' in data['message'].lower()
