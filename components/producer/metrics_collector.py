"""
Pure metrics collection module for event-log-demo.
Decouples metric gathering from Flask routes and makes logic testable.
"""
from typing import Dict, Callable, Any, Optional
import time
from kafka import KafkaConsumer, TopicPartition


def collect_metrics(providers: Dict[str, Callable[[], int]]) -> Dict[str, int]:
    """
    Pure pipeline: invoke each provider safely and collect results.
    
    Args:
        providers: Dict mapping metric names to provider functions.
    
    Returns:
        Dict with metric names and their values (or -1 on error).
    """
    # Guard: validate input
    if not providers or not isinstance(providers, dict):
        return {}
    
    results = {}
    for name, provider in providers.items():
        # Guard: validate provider is callable
        if not callable(provider):
            print(f"Provider for {name} is not callable", flush=True)
            results[name] = -1
            continue
        
        try:
            value = provider()
            # Guard: ensure return value is numeric
            results[name] = int(value) if isinstance(value, (int, float)) else -1
        except Exception as e:
            print(f"Failed to collect {name}: {e}", flush=True)
            results[name] = -1
    return results


def build_kafka_offset_provider(bootstrap_servers: list, topic: str, partition: int = 0) -> Callable[[], int]:
    """
    Factory: returns a function that queries Kafka end offset.
    
    Returns:
        A no-arg callable that returns the offset count.
    """
    # Guard: validate inputs
    if not bootstrap_servers or not isinstance(bootstrap_servers, list):
        def error_provider() -> int:
            return -1
        return error_provider
    
    if not topic or not isinstance(topic, str):
        def error_provider() -> int:
            return -1
        return error_provider
    
    if not isinstance(partition, int) or partition < 0:
        partition = 0
    
    def provider() -> int:
        consumer = None
        max_retries = 2
        for attempt in range(max_retries):
            try:
                consumer = KafkaConsumer(
                    bootstrap_servers=bootstrap_servers,
                    request_timeout_ms=5000,
                    api_version_auto_timeout_ms=5000
                )
                tp = TopicPartition(topic, partition)
                end_offsets = consumer.end_offsets([tp])
                count = end_offsets.get(tp, 0)
                return max(0, int(count))  # Ensure non-negative
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Kafka offset query failed (attempt {attempt + 1}/{max_retries}): {e}", flush=True)
                    time.sleep(0.5)
                else:
                    print(f"Kafka offset query failed after {max_retries} attempts: {e}", flush=True)
                    return -1
            finally:
                if consumer:
                    try:
                        consumer.close()
                    except Exception:
                        pass  # Ignore close errors
                    consumer = None
        return -1
    return provider


def build_cassandra_count_provider(session, keyspace: str, table: str) -> Callable[[], int]:
    """
    Factory: returns a function that queries Cassandra row count.
    
    Args:
        session: Active Cassandra session.
        keyspace: Keyspace name.
        table: Table name.
    
    Returns:
        A no-arg callable that returns the row count.
    """
    # Guard: validate string inputs
    if not keyspace or not isinstance(keyspace, str):
        def error_provider() -> int:
            return 0
        return error_provider
    
    if not table or not isinstance(table, str):
        def error_provider() -> int:
            return 0
        return error_provider
    
    def provider() -> int:
        # Guard: check session availability
        if not session:
            return 0  # Return 0 when Cassandra is not available
        
        max_retries = 2
        for attempt in range(max_retries):
            try:
                row = session.execute(f"SELECT count(*) FROM {keyspace}.{table}")
                # Cassandra returns ResultSet, get first row
                result = list(row)
                if result and len(result) > 0:
                    count = result[0].count
                    return max(0, int(count))  # Ensure non-negative
                return 0
            except Exception as e:
                if attempt < max_retries - 1:
                    print(f"Cassandra count query failed (attempt {attempt + 1}/{max_retries}): {e}", flush=True)
                    time.sleep(0.5)
                else:
                    print(f"Cassandra count query failed after {max_retries} attempts: {e}", flush=True)
                    return 0
        return 0
    return provider


def build_producer_count_provider(producer_instance) -> Callable[[], int]:
    """
    Factory: returns a function that reads the producer's generated_count.
    
    Args:
        producer_instance: The LogProducer instance.
    
    Returns:
        A no-arg callable that returns the count.
    """
    # Guard: validate producer instance
    if not producer_instance:
        def error_provider() -> int:
            return 0
        return error_provider
    
    def provider() -> int:
        try:
            count = getattr(producer_instance, 'generated_count', 0)
            return max(0, int(count))  # Ensure non-negative integer
        except (AttributeError, TypeError, ValueError):
            return 0
    return provider
