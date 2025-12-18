"""
Unit tests for bottleneck detector.
TDD: Tests document bottleneck detection logic.
Clean Code: Pure functions easy to test.
"""
import pytest
from src.bottleneck_detector import (
    BottleneckDetector,
    ComponentMetrics,
    BottleneckLag,
    Bottleneck
)


class TestBottleneckLag:
    """Test lag calculation value object."""
    
    def test_is_critical_when_above_threshold(self):
        """Lag above threshold should be critical."""
        lag = BottleneckLag(messages=3000, seconds=75.0)
        assert lag.is_critical(threshold_seconds=60.0) is True
    
    def test_is_not_critical_when_below_threshold(self):
        """Lag below threshold should not be critical."""
        lag = BottleneckLag(messages=1000, seconds=45.0)
        assert lag.is_critical(threshold_seconds=60.0) is False


class TestBottleneckDetector:
    """Test bottleneck detector logic."""
    
    def test_calculate_lag_basic(self):
        """Lag calculation should be accurate."""
        detector = BottleneckDetector(lag_threshold_seconds=60.0)
        lag = detector.calculate_lag(
            upstream_delta=1000,
            downstream_delta=800,
            current_rate=50.0
        )
        assert lag.messages == 200
        assert lag.seconds == 4.0
    
    def test_calculate_lag_prevents_division_by_zero(self):
        """Zero or negative rate should not cause division by zero."""
        detector = BottleneckDetector(lag_threshold_seconds=60.0)
        lag = detector.calculate_lag(
            upstream_delta=1000,
            downstream_delta=800,
            current_rate=0.0
        )
        assert lag.seconds == 200.0  # Falls back to rate=1.0
    
    def test_detect_producer_kafka_lag_when_critical(self):
        """Should detect bottleneck when lag exceeds threshold."""
        detector = BottleneckDetector(lag_threshold_seconds=60.0)
        bottleneck = detector.detect_producer_kafka_lag(
            producer_delta=5000,
            kafka_delta=1000,
            current_rate=50.0
        )
        assert bottleneck is not None
        assert bottleneck.component == "Producer→Kafka"
        assert "4000 messages" in bottleneck.issue
        assert bottleneck.severity in ["high", "critical"]
    
    def test_detect_producer_kafka_lag_when_not_critical(self):
        """Should not detect bottleneck when lag below threshold."""
        detector = BottleneckDetector(lag_threshold_seconds=60.0)
        bottleneck = detector.detect_producer_kafka_lag(
            producer_delta=1000,
            kafka_delta=900,
            current_rate=50.0
        )
        assert bottleneck is None  # 100 msgs / 50 msg/s = 2s lag (below 60s threshold)
    
    def test_detect_high_failure_rate_when_critical(self):
        """Should detect high failure rate."""
        detector = BottleneckDetector(lag_threshold_seconds=60.0)
        bottleneck = detector.detect_high_failure_rate(
            total_sent=800,
            total_failed=200,
            failure_threshold=0.1
        )
        assert bottleneck is not None
        assert bottleneck.component == "Producer"
        assert "20%" in bottleneck.issue
        assert bottleneck.severity == "critical"
    
    def test_detect_high_failure_rate_when_acceptable(self):
        """Should not detect bottleneck when failure rate acceptable."""
        detector = BottleneckDetector(lag_threshold_seconds=60.0)
        bottleneck = detector.detect_high_failure_rate(
            total_sent=950,
            total_failed=50,
            failure_threshold=0.1
        )
        assert bottleneck is None  # 5% failure rate (below 10% threshold)
    
    def test_detect_all_bottlenecks_none(self):
        """Should return empty list when no bottlenecks."""
        detector = BottleneckDetector(lag_threshold_seconds=60.0)
        baseline = ComponentMetrics(
            producer_count=0,
            kafka_count=1000,
            cassandra_count=1000,
            archive_count=1000
        )
        current = ComponentMetrics(
            producer_count=0,
            kafka_count=2000,
            cassandra_count=2000,
            archive_count=2000
        )
        bottlenecks = detector.detect_all_bottlenecks(
            baseline=baseline,
            current=current,
            total_sent=1000,
            total_failed=0,
            current_rate=50.0
        )
        assert len(bottlenecks) == 0
    
    def test_detect_all_bottlenecks_multiple(self):
        """Should detect multiple bottlenecks."""
        detector = BottleneckDetector(lag_threshold_seconds=60.0)
        baseline = ComponentMetrics(
            producer_count=0,
            kafka_count=1000,
            cassandra_count=1000,
            archive_count=1000
        )
        current = ComponentMetrics(
            producer_count=0,
            kafka_count=1100,  # Only 100 received vs 5000 sent
            cassandra_count=1050,  # 50 written vs 100 received
            archive_count=1010   # 10 archived vs 50 written
        )
        bottlenecks = detector.detect_all_bottlenecks(
            baseline=baseline,
            current=current,
            total_sent=5000,  # Huge producer delta
            total_failed=500,  # High failure rate
            current_rate=50.0
        )
        # Should detect: Producer→Kafka lag, Kafka→Cassandra lag, 
        # Cassandra→Archive lag, High failure rate
        assert len(bottlenecks) >= 2  # At least producer lag and failure rate
    
    def test_detector_requires_positive_threshold(self):
        """Constructor should reject negative threshold."""
        with pytest.raises(ValueError, match="must be positive"):
            BottleneckDetector(lag_threshold_seconds=-10.0)
