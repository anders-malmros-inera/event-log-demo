"""
Configuration management for processor service.
OWASP A05 compliant: No hardcoded credentials, all values from environment.
"""
import os
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class KafkaConfig:
    """Kafka connection configuration."""
    bootstrap_servers: str
    topic: str
    group_id: str
    auto_offset_reset: str = 'earliest'
    
    def __post_init__(self):
        """Validate configuration values."""
        if not self.bootstrap_servers:
            raise ValueError("Kafka bootstrap_servers cannot be empty")
        if not self.topic:
            raise ValueError("Kafka topic cannot be empty")
        if not self.group_id:
            raise ValueError("Kafka group_id cannot be empty")
        if self.auto_offset_reset not in ['earliest', 'latest']:
            raise ValueError("auto_offset_reset must be 'earliest' or 'latest'")


@dataclass(frozen=True)
class CassandraConfig:
    """Cassandra connection configuration."""
    host: str
    keyspace: str
    table: str
    
    def __post_init__(self):
        """Validate configuration values."""
        if not self.host:
            raise ValueError("Cassandra host cannot be empty")
        if not self.keyspace:
            raise ValueError("Cassandra keyspace cannot be empty")
        if not self.table:
            raise ValueError("Cassandra table cannot be empty")


@dataclass(frozen=True)
class AppConfig:
    """Application configuration."""
    kafka: KafkaConfig
    cassandra: CassandraConfig
    startup_delay_seconds: int = 10
    
    @classmethod
    def from_environment(cls) -> 'AppConfig':
        """
        Load configuration from environment variables.
        OWASP A05: Secure configuration from environment.
        
        Environment variables:
        - KAFKA_BOOTSTRAP_SERVERS (default: kafka:9092)
        - KAFKA_TOPIC (default: event-logs)
        - KAFKA_GROUP_ID (default: flink-processor-group)
        - CASSANDRA_HOST (default: cassandra)
        - CASSANDRA_KEYSPACE (default: logs)
        - CASSANDRA_TABLE (default: events)
        - STARTUP_DELAY_SECONDS (default: 10)
        """
        kafka_config = KafkaConfig(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'),
            topic=os.getenv('KAFKA_TOPIC', 'event-logs'),
            group_id=os.getenv('KAFKA_GROUP_ID', 'flink-processor-group'),
            auto_offset_reset=os.getenv('KAFKA_AUTO_OFFSET_RESET', 'earliest')
        )
        
        cassandra_config = CassandraConfig(
            host=os.getenv('CASSANDRA_HOST', 'cassandra'),
            keyspace=os.getenv('CASSANDRA_KEYSPACE', 'logs'),
            table=os.getenv('CASSANDRA_TABLE', 'events')
        )
        
        startup_delay = int(os.getenv('STARTUP_DELAY_SECONDS', '10'))
        if startup_delay < 0 or startup_delay > 300:
            raise ValueError("STARTUP_DELAY_SECONDS must be between 0 and 300")
        
        return cls(
            kafka=kafka_config,
            cassandra=cassandra_config,
            startup_delay_seconds=startup_delay
        )
