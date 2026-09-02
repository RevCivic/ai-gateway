"""
Test cases for models and data structures.
"""

import pytest
from models import (
    WorkerMetrics,
    QueueStats,
    RequestStatus,
    WorkerExclusionReason,
)


class TestWorkerMetrics:
    """Test WorkerMetrics class."""
    
    def test_success_rate_calculation(self):
        """Test success rate calculation."""
        metrics = WorkerMetrics(
            total_requests=100,
            successful_requests=80,
        )
        assert metrics.success_rate == 0.8
    
    def test_success_rate_no_requests(self):
        """Test success rate with no requests."""
        metrics = WorkerMetrics()
        assert metrics.success_rate == 0.0
    
    def test_error_rate_calculation(self):
        """Test error rate calculation."""
        metrics = WorkerMetrics(
            total_requests=100,
            successful_requests=80,
        )
        assert metrics.error_rate == 0.2
    
    def test_circuit_open_state(self):
        """Test circuit breaker state."""
        metrics = WorkerMetrics(circuit_open=True)
        assert metrics.circuit_open is True


class TestQueueStats:
    """Test QueueStats class."""
    
    def test_queue_utilization(self):
        """Test queue utilization calculation."""
        stats = QueueStats(
            pending_requests=50,
            max_queue_size=100,
        )
        assert stats.queue_utilization == 0.5
    
    def test_queue_is_full(self):
        """Test queue full detection."""
        stats = QueueStats(
            pending_requests=100,
            max_queue_size=100,
        )
        assert stats.is_full is True
    
    def test_queue_not_full(self):
        """Test queue not full detection."""
        stats = QueueStats(
            pending_requests=50,
            max_queue_size=100,
        )
        assert stats.is_full is False


class TestExclusionReasons:
    """Test WorkerExclusionReason enum."""
    
    def test_all_reasons_exist(self):
        """Test all exclusion reasons are defined."""
        reasons = [
            WorkerExclusionReason.CIRCUIT_BREAKER,
            WorkerExclusionReason.INELIGIBLE,
            WorkerExclusionReason.GPU_BUSY,
            WorkerExclusionReason.HEALTH_CHECK_FAILED,
            WorkerExclusionReason.OFFLINE,
        ]
        assert len(reasons) == 5
    
    def test_reason_string_value(self):
        """Test reason string values."""
        assert WorkerExclusionReason.CIRCUIT_BREAKER.value == "circuit_breaker"


class TestRequestStatus:
    """Test RequestStatus enum."""
    
    def test_all_statuses_exist(self):
        """Test all request statuses are defined."""
        statuses = [
            RequestStatus.PENDING,
            RequestStatus.ROUTED,
            RequestStatus.COMPLETED,
            RequestStatus.FAILED,
            RequestStatus.TIMEOUT,
        ]
        assert len(statuses) == 5
