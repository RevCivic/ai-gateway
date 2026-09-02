"""
Request queuing and load management for the AI Gateway.
"""

import asyncio
import time
from typing import Optional, TYPE_CHECKING
from dataclasses import dataclass

from models import QueueStats, RequestQueueFullError

if TYPE_CHECKING:
    from logging_utils import StructuredLogger


@dataclass
class QueuedRequest:
    """A request in the queue."""
    request_id: str
    model: str
    enqueue_time: float
    priority: int = 0  # Higher priority gets dequeued first


class RequestQueue:
    """Manages request queuing with backpressure support."""
    
    def __init__(
        self,
        max_size: int = 1000,
        logger: Optional["StructuredLogger"] = None,
    ):
        """
        Initialize request queue.
        
        Args:
            max_size: Maximum number of pending requests
            logger: Structured logger instance
        """
        self.max_size = max_size
        self.logger = logger
        self.queue: asyncio.PriorityQueue = asyncio.PriorityQueue(maxsize=max_size)
        
        # Statistics
        self.stats = QueueStats(max_queue_size=max_size)
    
    async def enqueue(
        self,
        request_id: str,
        model: str,
        priority: int = 0,
    ) -> None:
        """
        Enqueue a request.
        
        Args:
            request_id: Request ID
            model: Model name
            priority: Priority level (higher = dequeue first)
            
        Raises:
            RequestQueueFullError: If queue is at capacity
        """
        if self.stats.pending_requests >= self.max_size:
            if self.logger:
                self.logger.warning(
                    "Request queue full",
                    request_id=request_id,
                    model=model,
                    queue_size=self.stats.pending_requests,
                )
            raise RequestQueueFullError(
                f"Request queue full ({self.max_size} pending)"
            )
        
        queued = QueuedRequest(
            request_id=request_id,
            model=model,
            enqueue_time=time.time(),
            priority=priority,
        )
        
        try:
            # Use negative priority for max-heap behavior (higher priority first)
            self.queue.put_nowait((-priority, request_id, queued))
            self.stats.pending_requests += 1
            self.stats.requests_queued_total += 1
            
            if self.logger:
                self.logger.debug(
                    "Request enqueued",
                    request_id=request_id,
                    model=model,
                    queue_size=self.stats.pending_requests,
                    priority=priority,
                )
        
        except asyncio.QueueFull:
            if self.logger:
                self.logger.warning(
                    "Failed to enqueue request",
                    request_id=request_id,
                    model=model,
                )
            raise RequestQueueFullError("Queue is full")
    
    async def dequeue(self, timeout: Optional[float] = None) -> Optional[QueuedRequest]:
        """
        Dequeue a request.
        
        Args:
            timeout: Timeout in seconds to wait for a request
            
        Returns:
            Dequeued request or None if timeout
        """
        try:
            _, request_id, queued = await asyncio.wait_for(
                self.queue.get(),
                timeout=timeout,
            )
            self.stats.pending_requests = max(0, self.stats.pending_requests - 1)
            self.stats.requests_dequeued_total += 1
            
            wait_time = (time.time() - queued.enqueue_time) * 1000
            self._update_avg_wait_time(wait_time)
            
            if self.logger:
                self.logger.debug(
                    "Request dequeued",
                    request_id=queued.request_id,
                    model=queued.model,
                    queue_size=self.stats.pending_requests,
                    wait_time_ms=round(wait_time),
                )
            
            return queued
        
        except asyncio.TimeoutError:
            return None
    
    def get_stats(self) -> QueueStats:
        """Get queue statistics."""
        return self.stats
    
    def _update_avg_wait_time(self, wait_time_ms: float) -> None:
        """Update average wait time (exponential moving average)."""
        if self.stats.average_wait_time_ms == 0:
            self.stats.average_wait_time_ms = wait_time_ms
        else:
            # EMA: new_avg = 0.7 * old_avg + 0.3 * new_value
            self.stats.average_wait_time_ms = (
                0.7 * self.stats.average_wait_time_ms +
                0.3 * wait_time_ms
            )


class LoadBalancer:
    """Manages load balancing across multiple model classes."""
    
    def __init__(self, logger: Optional["StructuredLogger"] = None):
        """
        Initialize load balancer.
        
        Args:
            logger: Structured logger instance
        """
        self.logger = logger
        self.queues: dict[str, RequestQueue] = {}
        self.model_weights: dict[str, float] = {}
    
    def create_queue(
        self,
        model: str,
        max_size: int = 1000,
        weight: float = 1.0,
    ) -> None:
        """
        Create a queue for a model.
        
        Args:
            model: Model name
            max_size: Queue size
            weight: Weight for fair queuing (higher = more allocation)
        """
        self.queues[model] = RequestQueue(max_size, self.logger)
        self.model_weights[model] = max(0.1, weight)  # Minimum 0.1
        
        if self.logger:
            self.logger.debug(
                "Queue created",
                model=model,
                max_size=max_size,
                weight=weight,
            )
    
    async def enqueue_request(
        self,
        model: str,
        request_id: str,
    ) -> None:
        """
        Enqueue a request to a model's queue.
        
        Args:
            model: Model name
            request_id: Request ID
            
        Raises:
            RequestQueueFullError: If queue is full
            ValueError: If model queue doesn't exist
        """
        queue = self.queues.get(model)
        if queue is None:
            raise ValueError(f"No queue for model: {model}")
        
        await queue.enqueue(request_id, model)
    
    def get_queue_stats(self, model: str) -> Optional[QueueStats]:
        """Get queue statistics for a model."""
        queue = self.queues.get(model)
        if queue:
            return queue.get_stats()
        return None
    
    def get_all_stats(self) -> dict[str, QueueStats]:
        """Get statistics for all queues."""
        return {
            model: queue.get_stats()
            for model, queue in self.queues.items()
        }
    
    def is_overloaded(self) -> bool:
        """Check if any queue is at or near capacity."""
        return any(
            queue.stats.pending_requests >= queue.max_size * 0.9
            for queue in self.queues.values()
        )
    
    def get_utilization(self) -> dict[str, float]:
        """Get queue utilization per model (0.0 to 1.0)."""
        return {
            model: queue.stats.queue_utilization
            for model, queue in self.queues.items()
        }
