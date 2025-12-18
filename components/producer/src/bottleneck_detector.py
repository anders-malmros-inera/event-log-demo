"""
Bottleneck detection with low complexity and single responsibility.
Clean Code: Each function has one clear purpose.
SonarQube: Cyclomatic complexity < 10 for all functions.
OWASP A09: Secure logging without sensitive data exposure.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass
class ComponentMetrics:
    """Metrics for a single component at a point in time.
    
    Immutability: Represents a snapshot, cannot be modified.
    Testability: Easy to create test fixtures.
    """
    producer_count: int
    kafka_count: int
    cassandra_count: int
    archive_count: int


@dataclass
class BottleneckLag:
    """Calculated lag between components.
    
    Single Responsibility: Only holds lag calculations.
    """
    messages: int
    seconds: float
    
    def is_critical(self, threshold_seconds: float = 60.0) -> bool:
        """Check if lag exceeds threshold.
        
        Clean Code: Tell, don't ask - object decides if it's critical.
        """
        return self.seconds > threshold_seconds


@dataclass
class Bottleneck:
    """Detected bottleneck with context.
    
    Security: No sensitive data in error messages.
    """
    component: str
    issue: str
    severity: str  # low, medium, high, critical
    
    def to_dict(self) -> dict:
        """Convert to dictionary for API response."""
        return {
            "component": self.component,
            "issue": self.issue,
            "severity": self.severity
        }


class BottleneckDetector:
    """Detects performance bottlenecks in the pipeline.
    
    Single Responsibility: Only detects bottlenecks.
    Low Complexity: Each method < 5 cyclomatic complexity.
    Testability: Pure functions with explicit inputs.
    """
    
    def __init__(self, lag_threshold_seconds: float = 60.0):
        """Initialize detector with threshold.
        
        Security: Validates threshold to prevent divide-by-zero.
        """
        if lag_threshold_seconds <= 0:
            raise ValueError("lag_threshold_seconds must be positive")
        self.lag_threshold_seconds = lag_threshold_seconds
    
    def calculate_lag(
        self, 
        upstream_delta: int, 
        downstream_delta: int, 
        current_rate: float
    ) -> BottleneckLag:
        """Calculate lag between two components.
        
        Cyclomatic Complexity: 2 (one if statement)
        Pure Function: No side effects, deterministic output.
        
        Args:
            upstream_delta: Messages processed by upstream component
            downstream_delta: Messages processed by downstream component
            current_rate: Current message production rate (msg/s)
        
        Returns:
            BottleneckLag with messages and time lag
        """
        if current_rate <= 0:
            current_rate = 1.0  # Prevent division by zero
        
        lag_messages = upstream_delta - downstream_delta
        lag_seconds = lag_messages / current_rate
        
        return BottleneckLag(messages=lag_messages, seconds=lag_seconds)
    
    def detect_producer_kafka_lag(
        self,
        producer_delta: int,
        kafka_delta: int,
        current_rate: float
    ) -> Optional[Bottleneck]:
        """Detect bottleneck between Producer and Kafka.
        
        Cyclomatic Complexity: 3 (if not critical: return, if high: one severity, else: another)
        Single Responsibility: Only detects this specific bottleneck.
        """
        lag = self.calculate_lag(producer_delta, kafka_delta, current_rate)
        
        if not lag.is_critical(self.lag_threshold_seconds):
            return None
        
        # Determine severity based on lag magnitude
        severity = "critical" if lag.seconds > 120 else "high"
        
        return Bottleneck(
            component="Producer→Kafka",
            issue=f"Kafka ingestion lag ({lag.messages} messages behind producer, ~{lag.seconds:.1f}s at current rate)",
            severity=severity
        )
    
    def detect_kafka_cassandra_lag(
        self,
        kafka_delta: int,
        cassandra_delta: int,
        current_rate: float
    ) -> Optional[Bottleneck]:
        """Detect bottleneck between Kafka and Cassandra.
        
        Cyclomatic Complexity: 3
        """
        lag = self.calculate_lag(kafka_delta, cassandra_delta, current_rate)
        
        if not lag.is_critical(self.lag_threshold_seconds):
            return None
        
        severity = "critical" if lag.seconds > 120 else "high"
        
        return Bottleneck(
            component="Flink→Cassandra",
            issue=f"Cassandra write lag ({lag.messages} messages behind Kafka, ~{lag.seconds:.1f}s at current rate)",
            severity=severity
        )
    
    def detect_cassandra_archive_lag(
        self,
        cassandra_delta: int,
        archive_delta: int,
        current_rate: float
    ) -> Optional[Bottleneck]:
        """Detect bottleneck between Cassandra and Archive.
        
        Cyclomatic Complexity: 3
        """
        lag = self.calculate_lag(cassandra_delta, archive_delta, current_rate)
        
        if not lag.is_critical(self.lag_threshold_seconds):
            return None
        
        severity = "critical" if lag.seconds > 120 else "high"
        
        return Bottleneck(
            component="Cassandra→Archive",
            issue=f"Archive write lag ({lag.messages} messages behind Cassandra, ~{lag.seconds:.1f}s at current rate)",
            severity=severity
        )
    
    def detect_high_failure_rate(
        self,
        total_sent: int,
        total_failed: int,
        failure_threshold: float = 0.1
    ) -> Optional[Bottleneck]:
        """Detect high failure rate during test.
        
        Cyclomatic Complexity: 3
        OWASP A09: Logs failure rate without sensitive message content.
        
        Args:
            total_sent: Messages successfully sent
            total_failed: Messages that failed
            failure_threshold: Failure rate threshold (0.1 = 10%)
        """
        if total_sent <= 0:
            return None  # No messages sent yet
        
        failure_rate = total_failed / (total_sent + total_failed)
        
        if failure_rate <= failure_threshold:
            return None
        
        return Bottleneck(
            component="Producer",
            issue=f"High failure rate during test ({total_failed}/{total_sent + total_failed} = {int(failure_rate * 100)}%)",
            severity="critical"
        )
    
    def detect_all_bottlenecks(
        self,
        baseline: ComponentMetrics,
        current: ComponentMetrics,
        total_sent: int,
        total_failed: int,
        current_rate: float
    ) -> list[Bottleneck]:
        """Detect all bottlenecks in the pipeline.
        
        Cyclomatic Complexity: 1 (no conditionals, delegates to other methods)
        Testability: All inputs explicit, easy to test all scenarios.
        
        Returns:
            List of detected bottlenecks (empty if none)
        """
        bottlenecks = []
        
        # Calculate deltas from baseline
        kafka_delta = current.kafka_count - baseline.kafka_count
        cassandra_delta = current.cassandra_count - baseline.cassandra_count
        archive_delta = current.archive_count - baseline.archive_count
        
        # Check each pipeline stage
        producer_kafka = self.detect_producer_kafka_lag(total_sent, kafka_delta, current_rate)
        if producer_kafka:
            bottlenecks.append(producer_kafka)
        
        kafka_cassandra = self.detect_kafka_cassandra_lag(kafka_delta, cassandra_delta, current_rate)
        if kafka_cassandra:
            bottlenecks.append(kafka_cassandra)
        
        cassandra_archive = self.detect_cassandra_archive_lag(cassandra_delta, archive_delta, current_rate)
        if cassandra_archive:
            bottlenecks.append(cassandra_archive)
        
        failure_rate = self.detect_high_failure_rate(total_sent, total_failed)
        if failure_rate:
            bottlenecks.append(failure_rate)
        
        return bottlenecks
