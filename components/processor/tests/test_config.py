"""
Unit tests for processor configuration.
Tests OWASP A05 compliance and validation.
"""
import pytest
import os
from src.config import AppConfig, KafkaConfig, CassandraConfig


def test_kafka_config_valid():
    """Should create valid Kafka configuration."""
    config = KafkaConfig(
        bootstrap_servers='localhost:9092',
        topic='test-topic',
        group_id='test-group'
    )
    assert config.bootstrap_servers == 'localhost:9092'
    assert config.topic == 'test-topic'
    assert config.group_id == 'test-group'
    assert config.auto_offset_reset == 'earliest'


def test_kafka_config_empty_bootstrap_servers():
    """OWASP A03: Empty bootstrap servers should be rejected."""
    with pytest.raises(ValueError, match="bootstrap_servers cannot be empty"):
        KafkaConfig(
            bootstrap_servers='',
            topic='test-topic',
            group_id='test-group'
        )


def test_kafka_config_invalid_auto_offset():
    """Should reject invalid auto_offset_reset values."""
    with pytest.raises(ValueError, match="must be 'earliest' or 'latest'"):
        KafkaConfig(
            bootstrap_servers='localhost:9092',
            topic='test-topic',
            group_id='test-group',
            auto_offset_reset='invalid'
        )


def test_cassandra_config_valid():
    """Should create valid Cassandra configuration."""
    config = CassandraConfig(
        host='localhost',
        keyspace='test_keyspace',
        table='test_table'
    )
    assert config.host == 'localhost'
    assert config.keyspace == 'test_keyspace'
    assert config.table == 'test_table'


def test_cassandra_config_empty_host():
    """OWASP A03: Empty host should be rejected."""
    with pytest.raises(ValueError, match="host cannot be empty"):
        CassandraConfig(
            host='',
            keyspace='test_keyspace',
            table='test_table'
        )


def test_app_config_from_environment_defaults(monkeypatch):
    """OWASP A05: Should load default configuration from environment."""
    # Clear environment variables
    for key in ['KAFKA_BOOTSTRAP_SERVERS', 'CASSANDRA_HOST']:
        monkeypatch.delenv(key, raising=False)
    
    config = AppConfig.from_environment()
    
    assert config.kafka.bootstrap_servers == 'kafka:9092'
    assert config.kafka.topic == 'event-logs'
    assert config.cassandra.host == 'cassandra'
    assert config.cassandra.keyspace == 'logs'
    assert config.startup_delay_seconds == 10


def test_app_config_from_environment_custom(monkeypatch):
    """OWASP A05: Should load custom configuration from environment."""
    monkeypatch.setenv('KAFKA_BOOTSTRAP_SERVERS', 'custom-kafka:9092')
    monkeypatch.setenv('CASSANDRA_HOST', 'custom-cassandra')
    monkeypatch.setenv('CASSANDRA_KEYSPACE', 'custom_logs')
    monkeypatch.setenv('STARTUP_DELAY_SECONDS', '5')
    
    config = AppConfig.from_environment()
    
    assert config.kafka.bootstrap_servers == 'custom-kafka:9092'
    assert config.cassandra.host == 'custom-cassandra'
    assert config.cassandra.keyspace == 'custom_logs'
    assert config.startup_delay_seconds == 5


def test_app_config_invalid_startup_delay(monkeypatch):
    """Should reject invalid startup delay."""
    monkeypatch.setenv('STARTUP_DELAY_SECONDS', '500')
    
    with pytest.raises(ValueError, match="must be between 0 and 300"):
        AppConfig.from_environment()


def test_app_config_immutable():
    """Configuration should be immutable (frozen dataclass)."""
    config = AppConfig.from_environment()
    
    with pytest.raises(Exception):  # FrozenInstanceError
        config.startup_delay_seconds = 999
