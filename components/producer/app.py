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

# Clean Code: Import refactored modules (OWASP A03, A05 compliant)
from src.config import AppConfig
from src.value_objects import LoadTestConfig
from src.bottleneck_detector import (
    BottleneckDetector,
    ComponentMetrics
)

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
    """Initialize MinIO client with defensive retry (OWASP A05: no hardcoded credentials)."""
    global minio_client
    try:
        config = AppConfig.from_environment()
        minio_client = Minio(
            config.minio.endpoint,
            access_key=config.minio.access_key,
            secret_key=config.minio.secret_key,
            secure=config.minio.secure
        )
        print("MinIO client initialized from environment config", flush=True)
    except ValueError as ve:
        print(f"MinIO configuration validation failed: {ve}", flush=True)
        minio_client = None
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
        """Initialize Kafka producer with exponential backoff retry (OWASP A05: config from environment)."""
        retry_delay = 1
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Attempting Kafka connection (attempt {attempt}/{max_retries})...", flush=True)
                config = AppConfig.from_environment()
                self.kafka_producer = KafkaProducer(
                    bootstrap_servers=[config.kafka.bootstrap_servers],
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                    request_timeout_ms=30000,  # Increased from 10s to 30s
                    max_block_ms=10000,  # Increased from 5s to 10s
                    retries=3,
                    # High-throughput batching settings
                    acks=1,  # Wait for leader acknowledgment only (faster than acks='all')
                    compression_type='lz4',  # Enable compression for better throughput
                    batch_size=32768,  # 32KB batches (increased from default 16KB)
                    linger_ms=10,  # Wait 10ms to batch messages together
                    buffer_memory=67108864  # 64MB buffer (increased from default 32MB)
                )
                print("Kafka connection established successfully", flush=True)
                return
            except ValueError as ve:
                print(f"Kafka configuration validation failed: {ve}", flush=True)
                self.kafka_producer = None
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
        """Initialize or reinitialize Cassandra connection with retry strategy (OWASP A05: config from environment)."""
        retry_delay = 1
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Attempting Cassandra connection (attempt {attempt}/{max_retries})...", flush=True)
                config = AppConfig.from_environment()
                cluster = Cluster(
                    [config.cassandra.host],
                    connect_timeout=10,
                    control_connection_timeout=10
                )
                self.cassandra_session = cluster.connect()
                
                # Ensure keyspace and table exist
                self.cassandra_session.execute(
                    f"CREATE KEYSPACE IF NOT EXISTS {config.cassandra.keyspace} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
                )
                self.cassandra_session.execute(
                    f"CREATE TABLE IF NOT EXISTS {config.cassandra.keyspace}.{config.cassandra.table} (uid text PRIMARY KEY, timestamp double, payload text)"
                )
                print("Cassandra connection established successfully", flush=True)
                return
            except ValueError as ve:
                print(f"Cassandra configuration validation failed: {ve}", flush=True)
                self.cassandra_session = None
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
                # Send async without waiting - let Kafka batch efficiently
                # Producer counts messages sent to internal buffer as "produced"
                # Kafka will handle batching (linger_ms=10) and compression (lz4)
                self.kafka_producer.send('event-logs', value=log)
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
    """Serve metrics with real-time producer counts."""
    # Create a copy of cached metrics
    response = dict(metrics_cache)
    
    # Override with real-time producer counts for accurate UI display during load testing
    response["producer_count"] = producer.generated_count
    response["successful_count"] = producer.successful_count
    response["unsuccessful_count"] = producer.unsuccessful_count
    response["is_streaming"] = stream_active or load_test_active
    
    return jsonify(response)

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

# Load testing state
load_test_active = False
load_test_thread = None
load_test_metrics = {
    "start_time": None,
    "elapsed_seconds": 0,
    "current_rate": 0,
    "total_sent": 0,
    "total_failed": 0,
    "bottlenecks": [],
    "baseline": {}  # Baseline metrics when test starts
}

# Bottleneck detector with configurable threshold (cyclomatic complexity < 3)
LAG_THRESHOLD_SECONDS = 60.0
bottleneck_detector = BottleneckDetector(lag_threshold_seconds=LAG_THRESHOLD_SECONDS)

def detect_bottleneck():
    """Detect bottlenecks using refactored low-complexity detector (SonarQube compliant)."""
    if not load_test_active or not load_test_metrics.get('baseline'):
        return []
    
    try:
        # Build baseline metrics
        baseline = load_test_metrics['baseline']
        baseline_metrics = ComponentMetrics(
            producer_count=baseline.get('test_start_producer', 0),
            kafka_count=baseline.get('kafka_count', 0),
            cassandra_count=baseline.get('cassandra_count', 0),
            archive_count=baseline.get('archive_count', 0)
        )
        
        # Build current metrics
        current_metrics = ComponentMetrics(
            producer_count=producer.generated_count,
            kafka_count=metrics_cache.get('kafka_count', 0),
            cassandra_count=metrics_cache.get('cassandra_count', 0),
            archive_count=metrics_cache.get('archive_count', 0)
        )
        
        # Detect bottlenecks using clean, testable detector
        total_sent = load_test_metrics.get('total_sent', 0)
        total_failed = load_test_metrics.get('total_failed', 0)
        current_rate = load_test_metrics.get('current_rate', 1.0)
        
        bottlenecks = bottleneck_detector.detect_all_bottlenecks(
            baseline=baseline_metrics,
            current=current_metrics,
            total_sent=total_sent,
            total_failed=total_failed,
            current_rate=current_rate
        )
        
        # Convert to dict format for JSON serialization
        return [{
            "component": b.component,
            "issue": b.issue,
            "severity": b.severity
        } for b in bottlenecks]
    
    except Exception as e:
        print(f"Error detecting bottlenecks: {e}", flush=True)
        return []

def run_load_test(config):
    """Execute load test with ramping."""
    global load_test_active, load_test_metrics
    
    start_rate = config.get('start_rate', 1)
    max_rate = config.get('max_rate', 100)
    ramp_duration = config.get('ramp_duration_seconds', 60)
    sustain_duration = config.get('sustain_duration_seconds', 60)
    size_kb = config.get('size_kb', 1)
    
    # Wait briefly for fresh metrics before capturing baseline
    print("[Load Test] Refreshing metrics for baseline...", flush=True)
    producer.refresh_metrics_cache()
    time.sleep(1)  # Give metrics time to populate
    
    # Capture baseline metrics at test start
    baseline = {
        'kafka_count': metrics_cache.get('kafka_count', 0),
        'cassandra_count': metrics_cache.get('cassandra_count', 0),
        'producer_count': producer.generated_count,
        'archive_count': metrics_cache.get('archive_count', 0),
        'test_start_producer': producer.generated_count  # Store separate baseline for test calculations
    }
    
    load_test_metrics = {
        "start_time": time.time(),
        "elapsed_seconds": 0,
        "current_rate": start_rate,
        "total_sent": 0,
        "total_failed": 0,
        "bottlenecks": [],
        "phase": "ramping",
        "baseline": baseline
    }
    
    print(f"[Load Test] Starting: {start_rate}→{max_rate} msgs/sec over {ramp_duration}s, sustain for {sustain_duration}s", flush=True)
    
    start_time = time.time()
    phase_end_time = start_time + ramp_duration
    
    while load_test_active:
        current_time = time.time()
        elapsed = current_time - start_time
        load_test_metrics["elapsed_seconds"] = int(elapsed)
        
        # Determine current phase and rate
        if elapsed < ramp_duration:
            # Ramping phase: linear increase
            progress = elapsed / ramp_duration
            current_rate = start_rate + (max_rate - start_rate) * progress
            load_test_metrics["phase"] = "ramping"
        elif elapsed < ramp_duration + sustain_duration:
            # Sustain phase: maintain max rate
            current_rate = max_rate
            load_test_metrics["phase"] = "sustaining"
        else:
            # Test complete
            load_test_metrics["phase"] = "completed"
            print(f"[Load Test] Completed after {int(elapsed)}s", flush=True)
            break
        
        load_test_metrics["current_rate"] = round(current_rate, 1)
        
        # Send burst of messages
        messages_this_second = int(current_rate)
        delay = 1.0 / current_rate if current_rate > 0 else 1.0
        
        for _ in range(messages_this_second):
            if not load_test_active:
                break
            
            try:
                log = producer.generate_log(size_kb)
                success = producer.send_log(log)
                
                if success:
                    load_test_metrics["total_sent"] += 1
                else:
                    load_test_metrics["total_failed"] += 1
                
                time.sleep(delay)
            except Exception as e:
                print(f"[Load Test] Send error: {e}", flush=True)
                load_test_metrics["total_failed"] += 1
        
        # Detect bottlenecks every 10 seconds
        if int(elapsed) % 10 == 0:
            bottlenecks = detect_bottleneck()
            if bottlenecks:
                load_test_metrics["bottlenecks"] = bottlenecks
                for b in bottlenecks:
                    print(f"[Load Test] Bottleneck detected: {b['component']} - {b['issue']} ({b['severity']})", flush=True)
    
    # Final flush at end of test - ensure all buffered messages sent
    if producer.kafka_producer:
        try:
            print("[Load Test] Flushing remaining messages...", flush=True)
            producer.kafka_producer.flush(timeout=10)
            time.sleep(2)  # Give Kafka time to process final batch
        except Exception as flush_error:
            print(f"[Load Test] Final flush warning: {flush_error}", flush=True)
    
    load_test_active = False
    print(f"[Load Test] Final: {load_test_metrics['total_sent']} sent, {load_test_metrics['total_failed']} failed", flush=True)

@app.route('/loadtest/start', methods=['POST'])
def start_load_test():
    """Start ramping load test with validated inputs (OWASP A03: injection prevention)."""
    global load_test_active, load_test_thread
    
    if load_test_active:
        return jsonify({"error": "Load test already running"}), 400
    
    data = request.get_json() or {}
    
    try:
        # Use value objects for validation (prevents invalid states)
        load_config = LoadTestConfig.from_raw_input(data)
        
        config = {
            'start_rate': load_config.start_rate.value,
            'max_rate': load_config.max_rate.value,
            'ramp_duration_seconds': load_config.ramp_duration.value,
            'sustain_duration_seconds': load_config.sustain_duration.value,
            'size_kb': load_config.size_kb.value
        }
        
        load_test_active = True
        load_test_thread = threading.Thread(target=run_load_test, args=(config,), daemon=True)
        load_test_thread.start()
        
        return jsonify({
            "status": "Load test started",
            "config": config
        })
    except ValueError as e:
        # OWASP A09: Safe error message (no stack trace to user)
        print(f"Load test validation failed: {e}", flush=True)
        return jsonify({"error": f"Invalid load test configuration: {str(e)}"}), 400

@app.route('/loadtest/stop', methods=['POST'])
def stop_load_test():
    """Stop active load test."""
    global load_test_active
    
    if not load_test_active:
        return jsonify({"status": "No active load test"}), 400
    
    load_test_active = False
    return jsonify({"status": "Load test stopping"})

@app.route('/loadtest/status', methods=['GET'])
def get_load_test_status():
    """Get load test metrics and bottleneck analysis."""
    
    # Calculate detailed bottleneck metrics for display
    bottleneck_debug = {}
    if load_test_active and load_test_metrics.get('baseline'):
        kafka_count = metrics_cache.get('kafka_count', 0)
        cassandra_count = metrics_cache.get('cassandra_count', 0)
        archive_count = metrics_cache.get('archive_count', 0)
        producer_count = producer.generated_count
        
        baseline = load_test_metrics['baseline']
        
        # Use total_sent from load test metrics for accurate producer delta
        producer_delta = load_test_metrics.get('total_sent', 0)
        
        bottleneck_debug = {
            "current_counts": {
                "producer": producer_count,
                "kafka": kafka_count,
                "cassandra": cassandra_count,
                "archive": archive_count
            },
            "baseline_counts": {
                "producer": baseline.get('test_start_producer', 0),
                "kafka": baseline.get('kafka_count', 0),
                "cassandra": baseline.get('cassandra_count', 0),
                "archive": baseline.get('archive_count', 0)
            },
            "deltas": {
                "producer": producer_delta,  # Use actual messages sent during test
                "kafka": kafka_count - baseline.get('kafka_count', 0),
                "cassandra": cassandra_count - baseline.get('cassandra_count', 0),
                "archive": archive_count - baseline.get('archive_count', 0)
            },
            "lags": {
                "producer_kafka": producer_delta - (kafka_count - baseline.get('kafka_count', 0)),
                "kafka_cassandra": (kafka_count - baseline.get('kafka_count', 0)) - (cassandra_count - baseline.get('cassandra_count', 0)),
                "cassandra_archive": (cassandra_count - baseline.get('cassandra_count', 0)) - (archive_count - baseline.get('archive_count', 0))
            },
            "current_rate": load_test_metrics.get('current_rate', 0)
        }
    
    return jsonify({
        "active": load_test_active,
        "metrics": load_test_metrics,
        "system_metrics": {
            "producer_count": metrics_cache.get('producer_count', 0),
            "kafka_count": metrics_cache.get('kafka_count', 0),
            "cassandra_count": metrics_cache.get('cassandra_count', 0),
            "archive_count": metrics_cache.get('archive_count', 0)
        },
        "bottleneck_debug": bottleneck_debug
    })

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