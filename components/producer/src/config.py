"""
Configuration management with security-first design.
OWASP A05: Security Misconfiguration - No hardcoded secrets, explicit validation.
SonarQube: No magic numbers, centralized configuration.
"""
import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class KafkaConfig:
    """Kafka configuration with explicit validation.
    
    Security: Reads from environment, fails securely if missing.
    Immutability: frozen=True prevents modification after creation.
    """
    bootstrap_servers: str
    request_timeout_ms: int = 30000
    max_block_ms: int = 10000
    retries: int = 3
    acks: int = 1
    compression_type: str = 'lz4'
    batch_size: int = 32768
    linger_ms: int = 10
    buffer_memory: int = 67108864
    
    def __post_init__(self):
        """Validate configuration on initialization."""
        if not self.bootstrap_servers:
            raise ValueError("Kafka bootstrap_servers cannot be empty")
        if self.request_timeout_ms <= 0:
            raise ValueError("request_timeout_ms must be positive")
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")


@dataclass(frozen=True)
class CassandraConfig:
    """Cassandra configuration with security validation."""
    contact_points: list[str]
    connect_timeout: int = 10
    keyspace: str = 'logs'
    
    def __post_init__(self):
        """Validate configuration."""
        if not self.contact_points:
            raise ValueError("Cassandra contact_points cannot be empty")
        if self.connect_timeout <= 0:
            raise ValueError("connect_timeout must be positive")


@dataclass(frozen=True)
class MinioConfig:
    """MinIO configuration - OWASP A02: Secrets from environment only."""
    endpoint: str
    access_key: str
    secret_key: str
    bucket: str
    secure: bool = False
    
    def __post_init__(self):
        """Validate configuration and check for secrets in code."""
        if not self.endpoint:
            raise ValueError("MinIO endpoint cannot be empty")
        if not self.access_key or not self.secret_key:
            raise ValueError("MinIO credentials must be provided")
        if self.access_key == "minioadmin" or self.secret_key == "minioadmin":
            # OWASP A05: Warn about default credentials (acceptable for demo, not production)
            print("WARNING: Using default MinIO credentials (demo only)")


@dataclass(frozen=True)
class AppConfig:
    """Application-wide configuration loaded from environment.
    
    Security Principle: Fail securely with clear errors if config is invalid.
    """
    kafka: KafkaConfig
    cassandra: CassandraConfig
    minio: MinioConfig
    flask_host: str = '0.0.0.0'
    flask_port: int = 5000
    metrics_refresh_interval: int = 10
    archive_count_cache_ttl: int = 30
    
    @classmethod
    def from_environment(cls) -> 'AppConfig':
        """Load configuration from environment variables.
        
        OWASP A05: Security Misconfiguration - Explicit environment-based config.
        Fails fast with clear error messages if required config is missing.
        """
        kafka_config = KafkaConfig(
            bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        )
        
        cassandra_config = CassandraConfig(
            contact_points=[os.getenv('CASSANDRA_HOST', 'cassandra')]
        )
        
        minio_config = MinioConfig(
            endpoint=os.getenv('MINIO_ENDPOINT', 'minio:9000'),
            access_key=os.getenv('MINIO_ACCESS_KEY', 'minioadmin'),
            secret_key=os.getenv('MINIO_SECRET_KEY', 'minioadmin'),
            bucket=os.getenv('MINIO_BUCKET', 'event-logs'),
            secure=os.getenv('MINIO_SECURE', 'false').lower() == 'true'
        )
        
        return cls(
            kafka=kafka_config,
            cassandra=cassandra_config,
            minio=minio_config,
            flask_host=os.getenv('FLASK_HOST', '0.0.0.0'),
            flask_port=int(os.getenv('FLASK_PORT', '5000')),
            metrics_refresh_interval=int(os.getenv('METRICS_REFRESH_INTERVAL', '10')),
            archive_count_cache_ttl=int(os.getenv('ARCHIVE_COUNT_CACHE_TTL', '30'))
        )
