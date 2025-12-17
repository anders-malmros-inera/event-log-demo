"""
Unit tests for metrics_collector module.
"""
import pytest
from metrics_collector import (
    collect_metrics,
    build_producer_count_provider,
    build_kafka_offset_provider,
    build_cassandra_count_provider
)


def test_collect_metrics_all_success():
    """Given all providers succeed, collect_metrics returns their values."""
    providers = {
        "producer": lambda: 42,
        "kafka": lambda: 100,
        "cassandra": lambda: 200
    }
    
    result = collect_metrics(providers)
    
    assert result == {
        "producer": 42,
        "kafka": 100,
        "cassandra": 200
    }


def test_collect_metrics_handles_failures():
    """Given a provider raises, collect_metrics returns -1 for that metric."""
    def failing_provider():
        raise RuntimeError("Simulated failure")
    
    providers = {
        "working": lambda: 10,
        "failing": failing_provider
    }
    
    result = collect_metrics(providers)
    
    assert result["working"] == 10
    assert result["failing"] == -1


def test_collect_metrics_empty_providers():
    """Given no providers, collect_metrics returns empty dict."""
    result = collect_metrics({})
    assert result == {}


def test_producer_count_provider():
    """Given a producer instance, the provider returns its generated_count."""
    class FakeProducer:
        def __init__(self):
            self.generated_count = 123
    
    producer = FakeProducer()
    provider = build_producer_count_provider(producer)
    
    assert provider() == 123


def test_cassandra_count_provider_success():
    """Given a valid session, the provider queries and returns count."""
    class FakeRow:
        def __init__(self):
            self.count = 456
    
    class FakeSession:
        def execute(self, query):
            return [FakeRow()]
    
    session = FakeSession()
    provider = build_cassandra_count_provider(session, "logs", "events")
    
    assert provider() == 456


def test_cassandra_count_provider_no_session():
    """Given no session, the provider raises an error."""
    provider = build_cassandra_count_provider(None, "logs", "events")
    
    with pytest.raises(RuntimeError, match="No Cassandra session"):
        provider()


# Note: Kafka provider tests would require mocking KafkaConsumer or using testcontainers.
# For now, we trust the integration test or manual verification.
# If needed, add a test with unittest.mock to verify the call chain.
