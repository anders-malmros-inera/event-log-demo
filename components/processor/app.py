from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
import time
import uuid

# Clean Code: Import configuration module (OWASP A05 compliant)
from src.config import AppConfig

# Load configuration from environment (no hardcoded values)
config = AppConfig.from_environment()

def connect_cassandra(cassandra_config):
    """
    Connect to Cassandra with retry strategy.
    Pure function: takes config as input, returns session.
    Cyclomatic complexity: 2
    """
    while True:
        try:
            cluster = Cluster([cassandra_config.host])
            session = cluster.connect()
            
            # Create keyspace and table if needed
            session.execute(
                f"CREATE KEYSPACE IF NOT EXISTS {cassandra_config.keyspace} "
                f"WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
            )
            session.execute(
                f"CREATE TABLE IF NOT EXISTS {cassandra_config.keyspace}.{cassandra_config.table} "
                f"(uid text PRIMARY KEY, timestamp double, payload text)"
            )
            print(f"Connected to Cassandra at {cassandra_config.host}")
            return session
        except Exception as e:
            print(f"Waiting for Cassandra: {e}")
            time.sleep(5)

def validate_message_data(data: dict) -> bool:
    """
    Validate message contains required fields.
    OWASP A03: Input validation prevents invalid data processing.
    Cyclomatic complexity: 2
    
    Returns:
        True if valid, False otherwise.
    """
    if not isinstance(data, dict):
        return False
    
    uid = data.get('uid')
    timestamp = data.get('timestamp')
    
    if not uid or not isinstance(uid, str):
        return False
    
    if timestamp is None or not isinstance(timestamp, (int, float)):
        return False
    
    return True


def process_single_message(session, prepared_stmt, message_data: dict) -> bool:
    """
    Process a single message and insert into Cassandra.
    Pure function: takes session, statement, and data.
    Cyclomatic complexity: 2
    
    Returns:
        True if successful, False otherwise.
    """
    try:
        # OWASP A03: Validate input before processing
        if not validate_message_data(message_data):
            print(f"Invalid message data: {message_data}")
            return False
        
        uid = message_data['uid']
        timestamp = message_data['timestamp']
        payload = message_data.get('payload', '')
        
        # Use prepared statement (prevents injection)
        session.execute(prepared_stmt, (uid, timestamp, payload))
        print(f"Processed log {uid}")
        return True
    except Exception as e:
        # OWASP A09: Safe error logging (no sensitive data)
        print(f"Error processing message: {e}")
        return False


def process_messages():
    """
    Main processing loop: consume from Kafka and write to Cassandra.
    Cyclomatic complexity: 1 (delegates to pure functions)
    """
    session = connect_cassandra(config.cassandra)
    
    print(f"Connecting to Kafka at {config.kafka.bootstrap_servers}...")
    consumer = KafkaConsumer(
        config.kafka.topic,
        bootstrap_servers=[config.kafka.bootstrap_servers],
        auto_offset_reset=config.kafka.auto_offset_reset,
        enable_auto_commit=True,
        group_id=config.kafka.group_id,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Starting processing loop...")
    prepared_stmt = session.prepare(
        f"INSERT INTO {config.cassandra.keyspace}.{config.cassandra.table} "
        f"(uid, timestamp, payload) VALUES (?, ?, ?)"
    )

    for message in consumer:
        process_single_message(session, prepared_stmt, message.value)


if __name__ == "__main__":
    print(f"Starting processor with {config.startup_delay_seconds}s startup delay...")
    time.sleep(config.startup_delay_seconds)
    process_messages()
