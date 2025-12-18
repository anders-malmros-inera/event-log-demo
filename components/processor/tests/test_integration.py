"""
Integration tests for processor service.
Tests the full message processing flow.
"""
import pytest
import json
from unittest.mock import Mock, MagicMock, patch
from src.config import AppConfig


def test_validate_message_data_valid():
    """Should accept valid message data."""
    from app import validate_message_data
    
    data = {
        'uid': 'test-uuid-123',
        'timestamp': 1234567890.123,
        'payload': 'test payload'
    }
    
    assert validate_message_data(data) is True


def test_validate_message_data_missing_uid():
    """OWASP A03: Should reject message with missing uid."""
    from app import validate_message_data
    
    data = {
        'timestamp': 1234567890.123,
        'payload': 'test payload'
    }
    
    assert validate_message_data(data) is False


def test_validate_message_data_invalid_timestamp():
    """OWASP A03: Should reject message with invalid timestamp."""
    from app import validate_message_data
    
    data = {
        'uid': 'test-uuid-123',
        'timestamp': 'not-a-number',
        'payload': 'test payload'
    }
    
    assert validate_message_data(data) is False


def test_process_single_message_success():
    """Should process valid message successfully."""
    from app import process_single_message
    
    mock_session = Mock()
    mock_prepared_stmt = Mock()
    
    data = {
        'uid': 'test-uuid-123',
        'timestamp': 1234567890.123,
        'payload': 'test payload'
    }
    
    result = process_single_message(mock_session, mock_prepared_stmt, data)
    
    assert result is True
    mock_session.execute.assert_called_once_with(
        mock_prepared_stmt, 
        ('test-uuid-123', 1234567890.123, 'test payload')
    )


def test_process_single_message_invalid_data():
    """OWASP A03: Should reject invalid message data."""
    from app import process_single_message
    
    mock_session = Mock()
    mock_prepared_stmt = Mock()
    
    data = {'uid': None}  # Invalid
    
    result = process_single_message(mock_session, mock_prepared_stmt, data)
    
    assert result is False
    mock_session.execute.assert_not_called()


def test_process_single_message_cassandra_error():
    """Should handle Cassandra errors gracefully (OWASP A05: fail secure)."""
    from app import process_single_message
    
    mock_session = Mock()
    mock_session.execute.side_effect = Exception("Cassandra unavailable")
    mock_prepared_stmt = Mock()
    
    data = {
        'uid': 'test-uuid-123',
        'timestamp': 1234567890.123,
        'payload': 'test payload'
    }
    
    result = process_single_message(mock_session, mock_prepared_stmt, data)
    
    assert result is False  # Should not crash, just return False


def test_config_from_environment_integration(monkeypatch):
    """Integration test: verify config can be loaded from environment."""
    monkeypatch.setenv('KAFKA_BOOTSTRAP_SERVERS', 'test-kafka:9092')
    monkeypatch.setenv('CASSANDRA_HOST', 'test-cassandra')
    
    config = AppConfig.from_environment()
    
    assert config.kafka.bootstrap_servers == 'test-kafka:9092'
    assert config.cassandra.host == 'test-cassandra'
