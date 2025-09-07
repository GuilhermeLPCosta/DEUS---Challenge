"""
Consolidated monitoring service combining logging, metrics, and health checks
"""

import json
import logging
import sys
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from threading import Lock
from typing import Any, Deque, Dict, List, Optional


# Logging Components
@dataclass
class LoggerConfig:
    """Configuration for structured logging"""

    level: str = "INFO"
    format_type: str = "json"  # json or text


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging"""

    def __init__(self, config: LoggerConfig):
        super().__init__()
        self.config = config

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON"""
        log_data = {
            "level": record.levelname,
            "message": record.getMessage(),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add extra fields from record
        extra_fields = {}
        for key, value in record.__dict__.items():
            if key not in {
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
                "getMessage",
                "exc_info",
                "exc_text",
                "stack_info",
            }:
                extra_fields[key] = value

        if extra_fields:
            log_data["extra"] = extra_fields

        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info) if record.exc_info else None,
            }

        if self.config.format_type == "json":
            return json.dumps(log_data, default=str)
        else:
            # Text format fallback
            return f"{log_data['timestamp']} [{record.levelname}] {log_data['module']}.{log_data['function']}:{log_data['line']} {log_data['message']}"


# Metrics Components
class MetricType(Enum):
    """Types of metrics that can be collected"""

    COUNTER = "counter"
    HISTOGRAM = "histogram"
    GAUGE = "gauge"


@dataclass
class MetricValue:
    """Individual metric value with timestamp"""

    value: float
    timestamp: datetime
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class Counter:
    """Counter metric that only increases"""

    name: str
    value: int = 0
    tags: Dict[str, str] = field(default_factory=dict)

    def increment(self, amount: int = 1):
        """Increment counter by amount"""
        self.value += amount


@dataclass
class Histogram:
    """Histogram metric for tracking distributions"""

    name: str
    values: Deque[MetricValue] = field(default_factory=lambda: deque(maxlen=1000))
    tags: Dict[str, str] = field(default_factory=dict)

    def record(self, value: float, tags: Dict[str, str] = None):
        """Record a value in the histogram"""
        metric_tags = {**self.tags}
        if tags:
            metric_tags.update(tags)

        self.values.append(MetricValue(value=value, timestamp=datetime.utcnow(), tags=metric_tags))

    def get_percentile(self, percentile: float) -> Optional[float]:
        """Get percentile value from histogram"""
        if not self.values:
            return None

        sorted_values = sorted([v.value for v in self.values])
        index = int(len(sorted_values) * percentile / 100)
        return sorted_values[min(index, len(sorted_values) - 1)]

    def get_average(self) -> Optional[float]:
        """Get average value from histogram"""
        if not self.values:
            return None

        return sum(v.value for v in self.values) / len(self.values)


@dataclass
class Gauge:
    """Gauge metric for tracking current values"""

    name: str
    value: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tags: Dict[str, str] = field(default_factory=dict)

    def set(self, value: float, tags: Dict[str, str] = None):
        """Set gauge value"""
        self.value = value
        self.timestamp = datetime.utcnow()
        if tags:
            self.tags.update(tags)


# Health Check Components
class HealthStatus(Enum):
    """Health status levels"""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"


@dataclass
class ComponentHealth:
    """Health status of a single component"""

    name: str
    status: HealthStatus
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    check_duration_ms: float = 0.0
    timestamp: datetime = field(default_factory=datetime.utcnow)
    error: Optional[str] = None


class HealthCheck(ABC):
    """Abstract base class for health checks"""

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    async def check(self) -> ComponentHealth:
        """Perform health check and return status"""
        pass


# Main Monitoring Service
class MonitoringService:
    """Consolidated monitoring service for logging, metrics, and health checks"""

    def __init__(self, config: LoggerConfig = None):
        self.config = config or LoggerConfig()

        # Metrics storage
        self._counters: Dict[str, Counter] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._lock = Lock()

        # Health checks
        self.health_checks: List[HealthCheck] = []

        # Setup logging
        self._setup_logging()

    def _setup_logging(self):
        """Setup structured logging configuration"""
        # Remove existing handlers
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(StructuredFormatter(self.config))

        # Configure root logger
        root_logger.setLevel(getattr(logging, self.config.level.upper()))
        root_logger.addHandler(console_handler)
        root_logger.propagate = False

    # Logging methods
    def get_logger(self, name: str) -> logging.Logger:
        """Get logger instance"""
        return logging.getLogger(name)

    # Metrics methods
    def increment_counter(self, name: str, amount: int = 1, tags: Dict[str, str] = None) -> None:
        """Increment a counter metric"""
        with self._lock:
            key = self._get_metric_key(name, tags)
            if key not in self._counters:
                self._counters[key] = Counter(name=name, tags=tags or {})
            self._counters[key].increment(amount)

    def record_histogram(self, name: str, value: float, tags: Dict[str, str] = None) -> None:
        """Record a value in a histogram metric"""
        with self._lock:
            key = self._get_metric_key(name, tags)
            if key not in self._histograms:
                self._histograms[key] = Histogram(name=name, tags=tags or {})
            self._histograms[key].record(value, tags)

    def set_gauge(self, name: str, value: float, tags: Dict[str, str] = None) -> None:
        """Set a gauge metric value"""
        with self._lock:
            key = self._get_metric_key(name, tags)
            if key not in self._gauges:
                self._gauges[key] = Gauge(name=name, tags=tags or {})
            self._gauges[key].set(value, tags)

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics as a dictionary"""
        with self._lock:
            return {
                "counters": {
                    key: {"name": counter.name, "value": counter.value, "tags": counter.tags}
                    for key, counter in self._counters.items()
                },
                "histograms": {
                    key: {
                        "name": histogram.name,
                        "count": len(histogram.values),
                        "average": histogram.get_average(),
                        "p50": histogram.get_percentile(50),
                        "p95": histogram.get_percentile(95),
                        "p99": histogram.get_percentile(99),
                        "tags": histogram.tags,
                    }
                    for key, histogram in self._histograms.items()
                },
                "gauges": {
                    key: {
                        "name": gauge.name,
                        "value": gauge.value,
                        "timestamp": gauge.timestamp.isoformat(),
                        "tags": gauge.tags,
                    }
                    for key, gauge in self._gauges.items()
                },
            }

    def _get_metric_key(self, name: str, tags: Dict[str, str] = None) -> str:
        """Generate a unique key for a metric based on name and tags"""
        if not tags:
            return name

        sorted_tags = sorted(tags.items())
        tag_string = ",".join(f"{k}={v}" for k, v in sorted_tags)
        return f"{name}[{tag_string}]"

    # Health check methods
    def add_health_check(self, check: HealthCheck) -> None:
        """Add a health check"""
        self.health_checks.append(check)

    async def check_health(self) -> Dict[str, Any]:
        """Run all health checks and return comprehensive status"""
        start_time = time.time()

        component_results = {}
        overall_status = HealthStatus.HEALTHY

        for check in self.health_checks:
            try:
                check_start = time.time()
                result = await check.check()
                check_duration = (time.time() - check_start) * 1000
                result.check_duration_ms = check_duration

                component_results[check.name] = result

                # Determine overall status
                if result.status == HealthStatus.UNHEALTHY:
                    overall_status = HealthStatus.UNHEALTHY
                elif result.status == HealthStatus.DEGRADED and overall_status == HealthStatus.HEALTHY:
                    overall_status = HealthStatus.DEGRADED

            except Exception as e:
                error_result = ComponentHealth(
                    name=check.name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Health check failed: {str(e)}",
                    error=str(e),
                )
                component_results[check.name] = error_result
                overall_status = HealthStatus.UNHEALTHY

        total_duration = (time.time() - start_time) * 1000

        return {
            "status": overall_status.value,
            "timestamp": datetime.utcnow().isoformat(),
            "duration_ms": round(total_duration, 2),
            "components": {
                name: {
                    "status": result.status.value,
                    "message": result.message,
                    "details": result.details,
                    "check_duration_ms": round(result.check_duration_ms, 2),
                    "timestamp": result.timestamp.isoformat(),
                    "error": result.error,
                }
                for name, result in component_results.items()
            },
            "summary": {
                "total_checks": len(self.health_checks),
                "healthy_checks": sum(1 for r in component_results.values() if r.status == HealthStatus.HEALTHY),
                "degraded_checks": sum(1 for r in component_results.values() if r.status == HealthStatus.DEGRADED),
                "unhealthy_checks": sum(1 for r in component_results.values() if r.status == HealthStatus.UNHEALTHY),
            },
        }


# Global monitoring service instance
_monitoring_service: Optional[MonitoringService] = None


def get_monitoring_service() -> MonitoringService:
    """Get global monitoring service instance"""
    global _monitoring_service
    if _monitoring_service is None:
        _monitoring_service = MonitoringService()
    return _monitoring_service


def get_logger(name: str) -> logging.Logger:
    """Get logger instance from monitoring service"""
    return get_monitoring_service().get_logger(name)