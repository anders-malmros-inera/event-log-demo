"""
Unit tests for value objects.
TDD: Tests document expected behavior and edge cases.
OWASP: Tests verify security boundaries (invalid input rejection).
"""
import pytest
from src.value_objects import (
    MessagesPerSecond,
    DurationSeconds,
    LogSizeKB,
    LoadTestConfig
)


class TestMessagesPerSecond:
    """Test message rate value object validation."""
    
    def test_valid_rate_accepted(self):
        """Valid rates should be accepted."""
        rate = MessagesPerSecond(50.0)
        assert rate.value == 50.0
    
    def test_negative_rate_rejected(self):
        """OWASP A03: Negative rates must be rejected."""
        with pytest.raises(ValueError, match="must be >="):
            MessagesPerSecond(-1.0)
    
    def test_zero_rate_rejected(self):
        """OWASP A03: Zero rate rejected to prevent division by zero."""
        with pytest.raises(ValueError, match="must be >="):
            MessagesPerSecond(0.0)
    
    def test_excessive_rate_rejected(self):
        """OWASP A03: Excessive rates rejected to prevent resource exhaustion."""
        with pytest.raises(ValueError, match="must be <="):
            MessagesPerSecond(100000.0)
    
    def test_to_delay_calculation(self):
        """Delay calculation should be accurate."""
        rate = MessagesPerSecond(10.0)
        assert rate.to_delay_seconds() == 0.1
    
    def test_immutable(self):
        """Value objects must be immutable."""
        rate = MessagesPerSecond(50.0)
        with pytest.raises(AttributeError):
            rate.value = 100.0


class TestDurationSeconds:
    """Test duration value object validation."""
    
    def test_valid_duration_accepted(self):
        """Valid durations should be accepted."""
        duration = DurationSeconds(60)
        assert duration.value == 60
    
    def test_negative_duration_rejected(self):
        """OWASP A03: Negative durations must be rejected."""
        with pytest.raises(ValueError, match="must be >="):
            DurationSeconds(-10)
    
    def test_zero_duration_rejected(self):
        """OWASP A03: Zero duration must be rejected."""
        with pytest.raises(ValueError, match="must be >="):
            DurationSeconds(0)
    
    def test_excessive_duration_rejected(self):
        """OWASP A03: Excessive durations rejected to prevent resource lock."""
        with pytest.raises(ValueError, match="must be <="):
            DurationSeconds(10000)
    
    def test_float_duration_rejected(self):
        """Durations must be integers."""
        with pytest.raises(TypeError, match="must be integer"):
            DurationSeconds(10.5)


class TestLogSizeKB:
    """Test log size value object validation."""
    
    def test_valid_size_accepted(self):
        """Valid sizes should be accepted."""
        size = LogSizeKB(10.0)
        assert size.value == 10.0
    
    def test_negative_size_rejected(self):
        """OWASP A03: Negative sizes must be rejected."""
        with pytest.raises(ValueError, match="must be >="):
            LogSizeKB(-1.0)
    
    def test_zero_size_rejected(self):
        """OWASP A03: Zero size must be rejected."""
        with pytest.raises(ValueError, match="must be >="):
            LogSizeKB(0.0)
    
    def test_excessive_size_rejected(self):
        """OWASP A03: Excessive sizes rejected to prevent memory issues."""
        with pytest.raises(ValueError, match="must be <="):
            LogSizeKB(2000.0)
    
    def test_to_bytes_conversion(self):
        """Byte conversion should be accurate."""
        size = LogSizeKB(1.0)
        assert size.to_bytes() == 1024


class TestLoadTestConfig:
    """Test load test configuration validation."""
    
    def test_valid_config_accepted(self):
        """Valid configurations should be accepted."""
        config = LoadTestConfig(
            start_rate=MessagesPerSecond(1.0),
            max_rate=MessagesPerSecond(50.0),
            ramp_duration=DurationSeconds(60),
            sustain_duration=DurationSeconds(60),
            log_size=LogSizeKB(1.0)
        )
        assert config.start_rate.value == 1.0
        assert config.max_rate.value == 50.0
    
    def test_start_greater_than_max_rejected(self):
        """OWASP A03: start_rate > max_rate must be rejected."""
        with pytest.raises(ValueError, match="must be <="):
            LoadTestConfig(
                start_rate=MessagesPerSecond(100.0),
                max_rate=MessagesPerSecond(50.0),
                ramp_duration=DurationSeconds(60),
                sustain_duration=DurationSeconds(60),
                log_size=LogSizeKB(1.0)
            )
    
    def test_from_raw_input_valid(self):
        """Valid raw input should create config."""
        data = {
            'start_rate': 1,
            'max_rate': 50,
            'ramp_duration_seconds': 60,
            'sustain_duration_seconds': 60,
            'size_kb': 1
        }
        config = LoadTestConfig.from_raw_input(data)
        assert config.start_rate.value == 1.0
    
    def test_from_raw_input_invalid_rate(self):
        """OWASP A03: Invalid raw input must be rejected with clear error."""
        data = {
            'start_rate': -10,
            'max_rate': 50,
            'ramp_duration_seconds': 60,
            'sustain_duration_seconds': 60,
            'size_kb': 1
        }
        with pytest.raises(ValueError, match="Invalid load test configuration"):
            LoadTestConfig.from_raw_input(data)
    
    def test_total_duration_calculation(self):
        """Total duration should sum ramp and sustain."""
        config = LoadTestConfig(
            start_rate=MessagesPerSecond(1.0),
            max_rate=MessagesPerSecond(50.0),
            ramp_duration=DurationSeconds(30),
            sustain_duration=DurationSeconds(20),
            log_size=LogSizeKB(1.0)
        )
        assert config.total_duration_seconds() == 50
