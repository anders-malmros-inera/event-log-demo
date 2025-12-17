"""
Tests for background metrics caching.
"""
import pytest
from unittest.mock import MagicMock, patch
import time


def test_metrics_cache_updates_on_refresh():
    """Given the producer calls refresh_metrics_cache, the cache is updated."""
    # Import here to avoid circular dependency with app initialization
    import sys
    sys.path.insert(0, '/app')
    
    from app import producer, metrics_cache
    
    # Mock the providers to return fixed values
    with patch('app.build_producer_count_provider') as mock_producer_prov, \
         patch('app.build_kafka_offset_provider') as mock_kafka_prov, \
         patch('app.build_cassandra_count_provider') as mock_cass_prov:
        
        mock_producer_prov.return_value = lambda: 100
        mock_kafka_prov.return_value = lambda: 200
        mock_cass_prov.return_value = lambda: 300
        
        producer.refresh_metrics_cache()
        
        # Verify cache was updated
        assert metrics_cache['producer_count'] == 100
        assert metrics_cache['kafka_count'] == 200
        assert metrics_cache['cassandra_count'] == 300


def test_monitor_endpoint_serves_cache():
    """Given /monitor is called, it returns cached metrics without querying services."""
    import sys
    sys.path.insert(0, '/app')
    
    from app import app, metrics_cache
    
    # Pre-populate cache
    metrics_cache['producer_count'] = 42
    metrics_cache['kafka_count'] = 84
    metrics_cache['cassandra_count'] = 126
    metrics_cache['is_streaming'] = True
    
    client = app.test_client()
    response = client.get('/monitor')
    
    assert response.status_code == 200
    data = response.get_json()
    assert data['producer_count'] == 42
    assert data['kafka_count'] == 84
    assert data['cassandra_count'] == 126
    assert data['is_streaming'] is True
