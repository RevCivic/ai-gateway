"""
Test cases for request queue functionality.
"""

import pytest
import asyncio
import time
from models import RequestQueueFullError
from queue import RequestQueue, LoadBalancer


class TestRequestQueue:
    """Test RequestQueue class."""
    
    @pytest.mark.asyncio
    async def test_enqueue_request(self, request_queue):
        """Test enqueueing a request."""
        await request_queue.enqueue("req-1", "fast")
        assert request_queue.stats.pending_requests == 1
    
    @pytest.mark.asyncio
    async def test_enqueue_multiple_requests(self, request_queue):
        """Test enqueueing multiple requests."""
        for i in range(5):
            await request_queue.enqueue(f"req-{i}", "fast")
        assert request_queue.stats.pending_requests == 5
    
    @pytest.mark.asyncio
    async def test_queue_full_error(self, logger):
        """Test queue full error."""
        queue = RequestQueue(max_size=2, logger=logger)
        await queue.enqueue("req-1", "fast")
        await queue.enqueue("req-2", "fast")
        
        with pytest.raises(RequestQueueFullError):
            await queue.enqueue("req-3", "fast")
    
    @pytest.mark.asyncio
    async def test_dequeue_request(self, request_queue):
        """Test dequeueing a request."""
        await request_queue.enqueue("req-1", "fast")
        dequeued = await request_queue.dequeue(timeout=1.0)
        
        assert dequeued is not None
        assert dequeued.request_id == "req-1"
        assert dequeued.model == "fast"
        assert request_queue.stats.pending_requests == 0
    
    @pytest.mark.asyncio
    async def test_dequeue_timeout(self, request_queue):
        """Test dequeue timeout when queue empty."""
        dequeued = await request_queue.dequeue(timeout=0.1)
        assert dequeued is None
    
    @pytest.mark.asyncio
    async def test_priority_ordering(self, request_queue):
        """Test that higher priority requests dequeue first."""
        await request_queue.enqueue("req-1", "fast", priority=1)
        await request_queue.enqueue("req-2", "fast", priority=3)
        await request_queue.enqueue("req-3", "fast", priority=2)
        
        # Should dequeue in priority order: 3, 2, 1
        first = await request_queue.dequeue(timeout=1.0)
        assert first.request_id == "req-2"  # priority 3
        
        second = await request_queue.dequeue(timeout=1.0)
        assert second.request_id == "req-3"  # priority 2
        
        third = await request_queue.dequeue(timeout=1.0)
        assert third.request_id == "req-1"  # priority 1
    
    @pytest.mark.asyncio
    async def test_wait_time_tracking(self, request_queue):
        """Test average wait time tracking."""
        await request_queue.enqueue("req-1", "fast")
        await asyncio.sleep(0.1)
        await request_queue.dequeue(timeout=1.0)
        
        # Wait time should be ~100ms
        assert request_queue.stats.average_wait_time_ms > 50


class TestLoadBalancer:
    """Test LoadBalancer class."""
    
    def test_create_queue(self, load_balancer):
        """Test creating a queue."""
        load_balancer.create_queue("fast", max_size=100, weight=2.0)
        assert "fast" in load_balancer.queues
        assert load_balancer.model_weights["fast"] == 2.0
    
    @pytest.mark.asyncio
    async def test_enqueue_to_model(self, load_balancer):
        """Test enqueueing to a model queue."""
        load_balancer.create_queue("fast")
        await load_balancer.enqueue_request("fast", "req-1")
        
        stats = load_balancer.get_queue_stats("fast")
        assert stats.pending_requests == 1
    
    @pytest.mark.asyncio
    async def test_enqueue_invalid_model(self, load_balancer):
        """Test enqueueing to non-existent model."""
        with pytest.raises(ValueError):
            await load_balancer.enqueue_request("invalid", "req-1")
    
    def test_get_all_stats(self, load_balancer):
        """Test getting all queue statistics."""
        load_balancer.create_queue("fast")
        load_balancer.create_queue("balanced")
        
        stats = load_balancer.get_all_stats()
        assert len(stats) == 2
        assert "fast" in stats
        assert "balanced" in stats
    
    def test_get_utilization(self, load_balancer):
        """Test getting queue utilization."""
        load_balancer.create_queue("fast", max_size=100)
        load_balancer.create_queue("balanced", max_size=50)
        
        # Manually set pending requests
        load_balancer.queues["fast"].stats.pending_requests = 50
        load_balancer.queues["balanced"].stats.pending_requests = 25
        
        utilization = load_balancer.get_utilization()
        assert utilization["fast"] == 0.5
        assert utilization["balanced"] == 0.5
    
    def test_is_overloaded(self, load_balancer):
        """Test overload detection."""
        load_balancer.create_queue("fast", max_size=100)
        
        # Set to 90% capacity
        load_balancer.queues["fast"].stats.pending_requests = 90
        assert load_balancer.is_overloaded() is True
        
        # Set to 80% capacity
        load_balancer.queues["fast"].stats.pending_requests = 80
        assert load_balancer.is_overloaded() is False
    
    def test_weight_enforcement(self, load_balancer):
        """Test that weights are enforced (minimum 0.1)."""
        load_balancer.create_queue("fast", weight=0.01)
        assert load_balancer.model_weights["fast"] == 0.1  # Minimum enforced
