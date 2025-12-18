"""
Value objects for load testing parameters.
OWASP A03: Injection Prevention - Make invalid states unrepresentable.
SonarQube: Eliminate magic numbers, explicit validation.
Clean Code: Tell, don't ask - validation happens at creation.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class MessagesPerSecond:
    """Value object for message rate with validation.
    
    Security: Prevents negative or zero rates that could cause division by zero.
    Immutability: frozen=True ensures rate cannot be changed after validation.
    """
    value: float
    
    MIN_RATE = 0.1
    MAX_RATE = 10000.0
    
    def __post_init__(self):
        """Validate rate on construction - fail fast principle."""
        if not isinstance(self.value, (int, float)):
            raise TypeError(f"Rate must be numeric, got {type(self.value)}")
        if self.value < self.MIN_RATE:
            raise ValueError(f"Rate must be >= {self.MIN_RATE} msg/s, got {self.value}")
        if self.value > self.MAX_RATE:
            raise ValueError(f"Rate must be <= {self.MAX_RATE} msg/s, got {self.value}")
    
    def to_delay_seconds(self) -> float:
        """Convert rate to delay between messages."""
        return 1.0 / self.value
    
    def __str__(self) -> str:
        return f"{self.value:.1f} msg/s"


@dataclass(frozen=True)
class DurationSeconds:
    """Value object for time duration with validation.
    
    Security: Prevents negative or excessive durations.
    """
    value: int
    
    MIN_DURATION = 1
    MAX_DURATION = 3600  # 1 hour max
    
    def __post_init__(self):
        """Validate duration on construction."""
        if not isinstance(self.value, int):
            raise TypeError(f"Duration must be integer seconds, got {type(self.value)}")
        if self.value < self.MIN_DURATION:
            raise ValueError(f"Duration must be >= {self.MIN_DURATION}s, got {self.value}")
        if self.value > self.MAX_DURATION:
            raise ValueError(f"Duration must be <= {self.MAX_DURATION}s, got {self.value}")
    
    def __str__(self) -> str:
        return f"{self.value}s"


@dataclass(frozen=True)
class LogSizeKB:
    """Value object for log message size with validation.
    
    Security: Prevents oversized payloads that could cause memory issues.
    """
    value: float
    
    MIN_SIZE = 0.1
    MAX_SIZE = 1024.0  # 1MB max
    
    def __post_init__(self):
        """Validate size on construction."""
        if not isinstance(self.value, (int, float)):
            raise TypeError(f"Size must be numeric KB, got {type(self.value)}")
        if self.value < self.MIN_SIZE:
            raise ValueError(f"Size must be >= {self.MIN_SIZE} KB, got {self.value}")
        if self.value > self.MAX_SIZE:
            raise ValueError(f"Size must be <= {self.MAX_SIZE} KB, got {self.value}")
    
    def to_bytes(self) -> int:
        """Convert KB to bytes for payload generation."""
        return int(self.value * 1024)
    
    def __str__(self) -> str:
        return f"{self.value} KB"


@dataclass(frozen=True)
class LoadTestConfig:
    """Configuration for load test with validated parameters.
    
    Security: All inputs validated through value objects.
    Testability: Easy to test with explicit value objects.
    """
    start_rate: MessagesPerSecond
    max_rate: MessagesPerSecond
    ramp_duration: DurationSeconds
    sustain_duration: DurationSeconds
    log_size: LogSizeKB
    
    def __post_init__(self):
        """Validate relationships between parameters."""
        if self.start_rate.value > self.max_rate.value:
            raise ValueError(
                f"start_rate ({self.start_rate}) must be <= max_rate ({self.max_rate})"
            )
    
    @classmethod
    def from_raw_input(cls, data: dict) -> 'LoadTestConfig':
        """Create config from raw user input with validation.
        
        OWASP A03: Input validation before use.
        Fails fast with clear error messages for invalid input.
        """
        try:
            return cls(
                start_rate=MessagesPerSecond(float(data.get('start_rate', 1))),
                max_rate=MessagesPerSecond(float(data.get('max_rate', 50))),
                ramp_duration=DurationSeconds(int(data.get('ramp_duration_seconds', 60))),
                sustain_duration=DurationSeconds(int(data.get('sustain_duration_seconds', 60))),
                log_size=LogSizeKB(float(data.get('size_kb', 1)))
            )
        except (ValueError, TypeError) as e:
            # OWASP A09: Log validation failure without sensitive data
            print(f"Load test config validation failed: {e}")
            raise ValueError(f"Invalid load test configuration: {e}")
    
    def total_duration_seconds(self) -> int:
        """Calculate total test duration."""
        return self.ramp_duration.value + self.sustain_duration.value
    
    def __str__(self) -> str:
        return (
            f"LoadTest({self.start_rate}→{self.max_rate} "
            f"ramp={self.ramp_duration} sustain={self.sustain_duration})"
        )
