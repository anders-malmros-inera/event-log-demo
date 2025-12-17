from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json
import time
import uuid

# Configuration
KAFKA_BROKER = 'kafka:9092'
TOPIC = 'event-logs'
CASSANDRA_HOST = 'cassandra'
KEYSPACE = 'logs'
TABLE = 'events'

def connect_cassandra():
    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect()
            session.execute(f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}")
            session.execute(f"CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE} (uid text PRIMARY KEY, timestamp double, payload text)")
            print("Connected to Cassandra")
            return session
        except Exception as e:
            print(f"Waiting for Cassandra: {e}")
            time.sleep(5)

def process_messages():
    session = connect_cassandra()
    
    print("Connecting to Kafka...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='flink-processor-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Starting processing loop...")
    prepared_stmt = session.prepare(f"INSERT INTO {KEYSPACE}.{TABLE} (uid, timestamp, payload) VALUES (?, ?, ?)")

    for message in consumer:
        try:
            data = message.value
            uid = data.get('uid')
            timestamp = data.get('timestamp')
            payload = data.get('payload')
            
            if uid and timestamp:
                session.execute(prepared_stmt, (uid, timestamp, payload))
                print(f"Processed log {uid}")
        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == "__main__":
    # Delay to ensure services are up
    time.sleep(10)
    process_messages()
