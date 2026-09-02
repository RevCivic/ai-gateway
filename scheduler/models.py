"""
Data models and error classes for the AI Gateway scheduler.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum


class WorkerExclusionReason(str, Enum):
    """Reasons why a worker was excluded from routing."""
    CIRCUIT_BREAKER = "circuit_breaker"
    INELIGIBLE = "ineligible"
    GPU_BUSY = "external_gpu_busy"
    HEALTH_CHECK_FAILED = "health_check_failed"
    OFFLINE = "offline"


class RequestStatus(str, Enum):
    """Status of a request."""
    PENDING = "pending"
    ROUTED = "routed"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"


@dataclass
class WorkerConfig:
    """Configuration for a worker."""
    hostname: str
    deployment: str
    tps: float  # Throughput in tokens per second
    timeout: float = 30.0
    retry_count: int = 1
    weight: float = 1.0  # Scoring weight (0.0 to 1.0+)
    enabled: bool = True


@dataclass
class WorkerMetrics:
    """Metrics for a worker."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    timeout_requests: int = 0
    avg_latency_ms: float = 0.0
    failure_count: int = 0
    circuit_open: bool = False
    last_failure_time: Optional[float] = None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate (0.0 to 1.0)."""
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests
    
    @property
    def error_rate(self) -> float:
        """Calculate error rate (0.0 to 1.0)."""
        return 1.0 - self.success_rate


@dataclass
class RankedCandidate:
    """A worker candidate ranked for routing."""
    worker: str
    deployment: str
    score: float
    tps: float
    inflight: int
    status: Dict[str, Any]
    metrics: Optional[WorkerMetrics] = None
    exclusion_reason: Optional[WorkerExclusionReason] = None


@dataclass
class RankingResult:
    """Result of worker ranking for a model."""
    logical_model: str
    candidates: List[RankedCandidate] = field(default_factory=list)
    status_map: Dict[str, Dict[str, Any]] = field(default_factory=dict)


@dataclass
class RequestMetrics:
    """Metrics for a single request."""
    request_id: str
    model: str
    worker: Optional[str] = None
    status: RequestStatus = RequestStatus.PENDING
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    total_latency_ms: float = 0.0
    time_to_first_byte_ms: Optional[float] = None
    error: Optional[str] = None
    
    @property
    def duration_ms(self) -> float:
        """Calculate request duration."""
        if self.start_time is None or self.end_time is None:
            return 0.0
        return (self.end_time - self.start_time) * 1000


@dataclass
class QueueStats:
    """Statistics about request queue."""
    pending_requests: int = 0
    max_queue_size: int = 1000
    requests_queued_total: int = 0
    requests_dequeued_total: int = 0
    average_wait_time_ms: float = 0.0
    
    @property
    def queue_utilization(self) -> float:
        """Queue utilization percentage (0.0 to 1.0)."""
        if self.max_queue_size == 0:
            return 0.0
        return self.pending_requests / self.max_queue_size
    
    @property
    def is_full(self) -> bool:
        """Check if queue is at capacity."""
        return self.pending_requests >= self.max_queue_size


class AIGatewayError(Exception):
    """Base exception for AI Gateway."""
    pass


class NoAvailableWorkersError(AIGatewayError):
    """Raised when no workers are available."""
    pass


class RequestQueueFullError(AIGatewayError):
    """Raised when request queue is full."""
    pass


class RequestTimeoutError(AIGatewayError):
    """Raised when request times out."""
    pass


class ResponseValidationError(AIGatewayError):
    """Raised when response validation fails."""
    pass
