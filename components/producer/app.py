from flask import Flask, request, jsonify, render_template
import uuid
import time
import requests
import random
import string
import threading
import datetime
import json
from kafka import KafkaProducer
from cassandra.cluster import Cluster
from apscheduler.schedulers.background import BackgroundScheduler
from minio import Minio
from metrics_collector import (
    collect_metrics,
    build_producer_count_provider,
    build_kafka_offset_provider,
    build_cassandra_count_provider
)
from container_controller import (
    stop_service,
    start_service,
    restart_service,
    pause_service,
    unpause_service,
    get_container_status
)
from chaos_nurse import get_chaos_nurse

app = Flask(__name__)

# Global control for streaming
stream_active = False
stream_thread = None

# In-memory metrics cache
metrics_cache = {
    "producer_count": 0,
    "kafka_count": 0,
    "cassandra_count": 0,
    "archive_count": 0,
    "is_streaming": False
}

# Global MinIO client
minio_client = None
archive_count_cache = {"count": 0, "last_update": 0}

def init_minio():
    """Initialize MinIO client with defensive retry."""
    global minio_client
    try:
        minio_client = Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        print("MinIO client initialized", flush=True)
    except Exception as e:
        print(f"Failed to initialize MinIO client: {e}", flush=True)
        minio_client = None

def count_archived_logs():
    """Count objects in MinIO archive bucket with caching (updates every 30 seconds)."""
    global archive_count_cache, minio_client
    
    current_time = time.time()
    # Return cached value if less than 30 seconds old
    if current_time - archive_count_cache["last_update"] < 30:
        return archive_count_cache["count"]
    
    if not minio_client:
        print(f"[Archive Count] MinIO client not initialized, returning cached: {archive_count_cache['count']}", flush=True)
        return archive_count_cache["count"]  # Return last known count
    
    try:
        # Guard: check if bucket exists
        if not minio_client.bucket_exists("event-logs"):
            print("[Archive Count] Bucket 'event-logs' does not exist", flush=True)
            return 0
        
        # Count objects in logs/ prefix
        count = 0
        objects = minio_client.list_objects("event-logs", prefix="logs/", recursive=True)
        for _ in objects:
            count += 1
        
        # Update cache
        archive_count_cache["count"] = count
        archive_count_cache["last_update"] = current_time
        print(f"[Archive Count] Updated cache: {count} objects", flush=True)
        
        return count
    except Exception as e:
        print(f"Failed to count archived logs: {e}", flush=True)
        return archive_count_cache["count"]  # Return last known count on error

class LogProducer:
    def __init__(self, ingestion_url="http://traefik/events"):
        self.ingestion_url = ingestion_url
        self.generated_count = 0
        self.successful_count = 0
        self.unsuccessful_count = 0
        self.kafka_producer = None
        self.cassandra_session = None
        self._init_connections()

    def _init_connections(self):
        """Initialize connections with retry strategies."""
        # Init Kafka with retry
        self._init_kafka_with_retry()
        
        # Init Cassandra with retry
        self._init_cassandra()
    
    def _init_kafka_with_retry(self, max_retries=3):
        """Initialize Kafka producer with exponential backoff retry."""
        retry_delay = 1
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Attempting Kafka connection (attempt {attempt}/{max_retries})...", flush=True)
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=['kafka:9092'],
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                    request_timeout_ms=10000,
                    max_block_ms=5000,
                    retries=3
                )
                print("Kafka connection established successfully", flush=True)
                return
            except Exception as e:
                print(f"Kafka connection attempt {attempt} failed: {e}", flush=True)
                if attempt < max_retries:
                    print(f"Retrying in {retry_delay} seconds...", flush=True)
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print(f"Kafka connection failed after {max_retries} attempts", flush=True)
                    self.kafka_producer = None

    def _init_cassandra(self, max_retries=3):
        """Initialize or reinitialize Cassandra connection with retry strategy."""
        retry_delay = 1
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Attempting Cassandra connection (attempt {attempt}/{max_retries})...", flush=True)
                cluster = Cluster(
                    ['cassandra'],
                    connect_timeout=10,
                    control_connection_timeout=10
                )
                self.cassandra_session = cluster.connect()
                
                # Ensure keyspace and table exist
                self.cassandra_session.execute(
                    "CREATE KEYSPACE IF NOT EXISTS logs WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
                )
                self.cassandra_session.execute(
                    "CREATE TABLE IF NOT EXISTS logs.events (uid text PRIMARY KEY, timestamp double, payload text)"
                )
                print("Cassandra connection established successfully", flush=True)
                return
            except Exception as e:
                print(f"Cassandra connection attempt {attempt} failed: {e}", flush=True)
                if attempt < max_retries:
                    print(f"Retrying in {retry_delay} seconds...", flush=True)
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print(f"Cassandra connection failed after {max_retries} attempts", flush=True)
                    self.cassandra_session = None

    def generate_log(self, size_kb):
        """Pure function: Generate a log dict with UID and random payload."""
        # Guard: validate input
        if not isinstance(size_kb, (int, float)) or size_kb <= 0:
            raise ValueError("size_kb must be a positive number")
        
        uid = str(uuid.uuid4())
        payload_size = max(1, int(size_kb * 1024))  # Ensure at least 1 byte
        payload = ''.join(random.choices(string.ascii_letters + string.digits, k=payload_size))
        return {"uid": uid, "timestamp": time.time(), "payload": payload}

    def send_log(self, log):
        """Tell-don't-ask: Producer handles sending with retry logic."""
        # Guard: validate input
        if not log or not isinstance(log, dict):
            self.unsuccessful_count += 1
            return False
        
        success = False
        
        # Try HTTP (Traefik) with timeout
        try:
            response = requests.post(self.ingestion_url, json=log, timeout=0.5)
            if response.status_code == 200:
                success = True
        except requests.exceptions.Timeout:
            print("HTTP request timeout to Traefik", flush=True)
        except requests.exceptions.ConnectionError as e:
            print(f"Connection error to Traefik: {e}", flush=True)
        except Exception as e:
            print(f"Unexpected error sending to Traefik: {e}", flush=True)

        # Fallback/Direct to Kafka for demo continuity
        if not success:
            success = self._send_to_kafka_with_retry(log)

        if success:
            self.generated_count += 1
            self.successful_count += 1
        else:
            self.unsuccessful_count += 1
        return success
    
    def _send_to_kafka_with_retry(self, log, max_retries=2):
        """Send to Kafka with retry and reconnection logic."""
        for attempt in range(1, max_retries + 1):
            # Check if Kafka producer exists
            if not self.kafka_producer:
                print(f"Kafka producer not available, attempting reconnection...", flush=True)
                self._init_kafka_with_retry(max_retries=1)
                if not self.kafka_producer:
                    continue
            
            try:
                future = self.kafka_producer.send('event-logs', value=log)
                # Wait for send to complete with timeout
                future.get(timeout=2)
                return True
            except Exception as e:
                print(f"Kafka send attempt {attempt} failed: {e}", flush=True)
                # Reset producer on error to force reconnection on next attempt
                if attempt < max_retries:
                    self.kafka_producer = None
                    time.sleep(0.5)
        
        print(f"Kafka send failed after {max_retries} attempts", flush=True)
        return False

    def stream_logs(self, size_kb, rate_per_sec):
        """Stream logs at specified rate. Defensively handles errors."""
        global stream_active
        
        # Guard: validate parameters
        if rate_per_sec <= 0:
            print("Invalid rate_per_sec, stopping stream", flush=True)
            stream_active = False
            return
        
        while stream_active:
            try:
                start_time = time.time()
                log = self.generate_log(size_kb)
                self.send_log(log)
                
                elapsed = time.time() - start_time
                sleep_time = max(0, (1.0 / rate_per_sec) - elapsed)
                time.sleep(sleep_time)
            except Exception as e:
                print(f"Error in stream loop: {e}", flush=True)
                time.sleep(0.1)  # Brief pause before retrying

    def refresh_metrics_cache(self):
        """Background job: refresh metrics and update the cache."""
        global metrics_cache
        
        # Health check and retry for Cassandra connection
        if self.cassandra_session is None:
            print("Cassandra session unavailable, attempting reconnection...", flush=True)
            self._init_cassandra(max_retries=1)
        else:
            # Verify session is actually usable
            try:
                self.cassandra_session.execute("SELECT now() FROM system.local", timeout=2.0)
            except Exception as e:
                print(f"Cassandra session unhealthy ({e}), attempting reconnection...", flush=True)
                self.cassandra_session = None
                self._init_cassandra(max_retries=1)
        
        # Health check for Kafka connection
        if self.kafka_producer is None:
            print("Kafka producer unavailable, attempting reconnection...", flush=True)
            self._init_kafka_with_retry(max_retries=1)
        
        try:
            providers = {
                "producer_count": build_producer_count_provider(self),
                "kafka_count": build_kafka_offset_provider(['kafka:9092'], 'event-logs', 0),
                "cassandra_count": build_cassandra_count_provider(self.cassandra_session, 'logs', 'events')
            }
            
            metrics = collect_metrics(providers)
            metrics["is_streaming"] = stream_active
            metrics["successful_count"] = self.successful_count
            metrics["unsuccessful_count"] = self.unsuccessful_count
            metrics["archive_count"] = count_archived_logs()  # Add archive count
            
            # Log connection status
            metrics["kafka_connected"] = self.kafka_producer is not None
            metrics["cassandra_connected"] = self.cassandra_session is not None
            
            # Update cache dict in-place to preserve references
            metrics_cache.clear()
            metrics_cache.update(metrics)
            
            # Log periodically (every 5th refresh = ~50 seconds)
            if hasattr(self, '_refresh_counter'):
                self._refresh_counter += 1
            else:
                self._refresh_counter = 1
            
            if self._refresh_counter % 5 == 0:
                print(f"Metrics refreshed: producer={metrics['producer_count']}, kafka={metrics['kafka_count']}, "
                      f"cassandra={metrics['cassandra_count']}, kafka_conn={metrics['kafka_connected']}, "
                      f"cassandra_conn={metrics['cassandra_connected']}", flush=True)
        except Exception as e:
            print(f"Error refreshing metrics: {e}", flush=True)

producer = LogProducer()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/start', methods=['POST'])
def start_stream():
    """Start log streaming with validated parameters."""
    global stream_active, stream_thread
    
    # Guard: already streaming
    if stream_active:
        return jsonify({"status": "Already streaming"}), 400
    
    # Input validation with defaults
    data = request.get_json() or {}
    
    try:
        size_kb = float(data.get('size_kb', 1))
        rate = float(data.get('rate', 1))
    except (TypeError, ValueError) as e:
        return jsonify({
            "status": "error",
            "message": "Invalid parameters: size_kb and rate must be numeric"
        }), 400
    
    # Guard: validate ranges
    if size_kb <= 0 or size_kb > 1000:
        return jsonify({
            "status": "error",
            "message": "size_kb must be between 0 and 1000"
        }), 400
    
    if rate <= 0 or rate > 10000:
        return jsonify({
            "status": "error",
            "message": "rate must be between 0 and 10000"
        }), 400
    
    # Reset counters before starting
    producer.generated_count = 0
    producer.successful_count = 0
    producer.unsuccessful_count = 0
    
    stream_active = True
    stream_thread = threading.Thread(target=producer.stream_logs, args=(size_kb, rate))
    stream_thread.daemon = True
    stream_thread.start()
    
    return jsonify({"status": "Stream started", "size_kb": size_kb, "rate": rate})

@app.route('/stop', methods=['POST'])
def stop_stream():
    global stream_active
    stream_active = False
    return jsonify({"status": "Stream stopped"})

@app.route('/monitor')
def monitor():
    """Serve cached metrics instantly without blocking."""
    return jsonify(metrics_cache)

@app.route('/control/<service>/<action>', methods=['POST'])
def control_service(service, action):
    """
    Control container lifecycle for demo scenarios.
    
    POST /control/flink/stop - Stop Flink job manager
    POST /control/kafka/start - Start Kafka broker
    POST /control/producer/pause - Pause producer container
    """
    allowed_services = ['flink', 'kafka', 'zookeeper', 'cassandra', 'flink-taskmanager']
    allowed_actions = ['stop', 'start', 'restart', 'pause', 'unpause']
    
    if service not in allowed_services:
        return jsonify({
            "success": False,
            "message": f"Service '{service}' not allowed. Choose from: {', '.join(allowed_services)}"
        }), 400
    
    if action not in allowed_actions:
        return jsonify({
            "success": False,
            "message": f"Action '{action}' not allowed. Choose from: {', '.join(allowed_actions)}"
        }), 400
    
    # Execute the control action
    action_map = {
        'stop': stop_service,
        'start': start_service,
        'restart': restart_service,
        'pause': pause_service,
        'unpause': unpause_service
    }
    
    success, message = action_map[action](service)
    
    return jsonify({
        "success": success,
        "message": message,
        "service": service,
        "action": action
    }), 200 if success else 500

@app.route('/control/<service>/status', methods=['GET'])
def service_status(service):
    """Get the current status of a service."""
    allowed_services = ['flink', 'kafka', 'zookeeper', 'cassandra', 'flink-taskmanager']
    
    if service not in allowed_services:
        return jsonify({
            "error": f"Service '{service}' not allowed"
        }), 400
    
    status = get_container_status(service)
    return jsonify(status)

@app.route('/chaos/enable', methods=['POST'])
def enable_chaos():
    """Enable Chaos Nurse to start random service disruptions."""
    nurse = get_chaos_nurse()
    nurse.enable()
    return jsonify({
        "success": True,
        "message": "Chaos Nurse enabled - random service disruptions every 60 seconds",
        "status": nurse.get_status()
    })

@app.route('/chaos/disable', methods=['POST'])
def disable_chaos():
    """Disable Chaos Nurse to stop service disruptions."""
    nurse = get_chaos_nurse()
    nurse.disable()
    return jsonify({
        "success": True,
        "message": "Chaos Nurse disabled - no more random disruptions",
        "status": nurse.get_status()
    })

@app.route('/chaos/status', methods=['GET'])
def chaos_status():
    """Get Chaos Nurse status and recent action history."""
    nurse = get_chaos_nurse()
    return jsonify(nurse.get_status())

@app.route('/chaos/history', methods=['GET'])
def chaos_history():
    """Get full Chaos Nurse action history."""
    nurse = get_chaos_nurse()
    status = nurse.get_status()
    return jsonify({
        "enabled": status['enabled'],
        "history": status['recent_actions']
    })

@app.route('/chaos/trigger', methods=['POST'])
def trigger_chaos():
    """Manually trigger a chaos action immediately."""
    nurse = get_chaos_nurse()
    result = nurse.execute_chaos_action()
    return jsonify(result)

@app.route('/logs/<component>')
def get_component_logs(component):
    """Generate simulated logs for the requested component."""
    logs = []
    now = datetime.datetime.now()
    
    for i in range(10):
        timestamp = (now - datetime.timedelta(seconds=i*0.5)).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        
        if component == 'producer':
            msg = f"Generated log event {uuid.uuid4()} - Size: {random.randint(1,5)}KB"
        elif component == 'traefik':
            msg = f"POST /events HTTP/1.1 200 {random.randint(10, 50)}ms"
        elif component == 'kafka':
            msg = f"Message appended to partition event-logs-0 offset {random.randint(1000, 9999)}"
        elif component == 'flink':
            msg = f"Processing window {timestamp} - Aggregated {random.randint(1, 10)} events"
        elif component == 'cassandra':
            msg = f"INSERT INTO event_logs (id, payload) VALUES ({uuid.uuid4()}, ...)"
        elif component == 'presto':
            msg = f"SELECT count(*) FROM event_logs WHERE timestamp > {int(time.time()) - 60}"
        elif component == 'grafana':
            msg = f"Querying PrestoDB for dashboard 'Event Metrics' panel 2"
        else:
            msg = f"[{component.upper()}] System active"
            
        logs.append(f"[{timestamp}] [INFO] {msg}")
        
    return jsonify({"logs": logs})

@app.route('/config/retention', methods=['GET', 'POST'])
def configure_retention():
    """
    Get or set retention policies for Cassandra TTL and Kafka retention.
    
    Defensive: validates input ranges, provides sensible defaults.
    """
    if request.method == 'GET':
        # Return current configuration (from environment or defaults)
        import os
        cassandra_ttl_days = int(os.getenv('CASSANDRA_TTL_SECONDS', 5184000)) // 86400
        kafka_retention_days = int(os.getenv('KAFKA_RETENTION_DAYS', 8))
        
        return jsonify({
            "cassandra_ttl_days": cassandra_ttl_days,
            "kafka_retention_days": kafka_retention_days,
            "status": "Current retention policies"
        })
    
    elif request.method == 'POST':
        data = request.get_json() or {}
        
        # Guard: validate inputs
        cassandra_ttl_days = data.get('cassandra_ttl_days', 60)
        kafka_retention_days = data.get('kafka_retention_days', 8)
        
        # Defensive bounds: TTL between 1 and 365 days
        if not isinstance(cassandra_ttl_days, (int, float)) or cassandra_ttl_days < 1 or cassandra_ttl_days > 365:
            return jsonify({"error": "Cassandra TTL must be between 1 and 365 days"}), 400
        
        # Defensive bounds: Kafka retention between 1 and 30 days
        if not isinstance(kafka_retention_days, (int, float)) or kafka_retention_days < 1 or kafka_retention_days > 30:
            return jsonify({"error": "Kafka retention must be between 1 and 30 days"}), 400
        
        cassandra_ttl_seconds = int(cassandra_ttl_days * 86400)
        
        try:
            # Update Cassandra table TTL
            if producer.cassandra_session:
                alter_query = f"ALTER TABLE logs.events WITH default_time_to_live = {cassandra_ttl_seconds}"
                producer.cassandra_session.execute(alter_query)
                print(f"Updated Cassandra TTL to {cassandra_ttl_days} days", flush=True)
            
            # Note: Kafka retention is configured at topic level, typically done via admin API
            # For demo purposes, we acknowledge the setting
            
            return jsonify({
                "success": True,
                "message": "Retention policies updated",
                "cassandra_ttl_days": cassandra_ttl_days,
                "cassandra_ttl_seconds": cassandra_ttl_seconds,
                "kafka_retention_days": kafka_retention_days,
                "note": "Kafka retention requires topic reconfiguration (not implemented in demo)"
            })
        except Exception as e:
            print(f"Failed to update retention policies: {e}", flush=True)
            return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("Starting producer application...", flush=True)
    
    # Initialize metrics cache on startup
    print("Initializing metrics cache...", flush=True)
    producer.refresh_metrics_cache()
    
    # Start background scheduler to refresh metrics every 10 seconds
    print("Starting background scheduler...", flush=True)
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=producer.refresh_metrics_cache,
        trigger="interval",
        seconds=10,
        max_instances=1,
        misfire_grace_time=30
    )
    
    # Add Chaos Nurse job (runs every 60 seconds when enabled)
    def chaos_nurse_job():
        """Execute random chaos if Chaos Nurse is enabled."""
        nurse = get_chaos_nurse()
        if nurse.is_enabled():
            result = nurse.execute_chaos_action()
            if result.get('executed'):
                print(f"[Chaos Nurse] {result['action']} on {result['service']}: {result['message']}", flush=True)
    
    scheduler.add_job(
        func=chaos_nurse_job,
        trigger="interval",
        seconds=60,
        max_instances=1,
        misfire_grace_time=30
    )
    
    scheduler.start()
    print("Background scheduler started successfully (metrics + chaos nurse)", flush=True)
    
    # Initialize MinIO client
    init_minio()
    
    try:
        app.run(host='0.0.0.0', port=5000)
    finally:
        print("Shutting down scheduler...", flush=True)
        scheduler.shutdown()