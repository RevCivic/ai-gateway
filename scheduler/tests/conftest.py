"""
Test configuration and fixtures for AI Gateway tests.
"""

import pytest
import asyncio
from typing import Generator
from unittest.mock import AsyncMock, MagicMock

from logging_utils import get_logger
from config import Config, reset_config
from queue import RequestQueue, LoadBalancer
from models import WorkerMetrics


@pytest.fixture
def event_loop() -> Generator:
    """Provide an event loop for async tests."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    yield loop
    loop.close()


@pytest.fixture
def logger():
    """Provide a logger instance."""
    return get_logger("test")


@pytest.fixture
def config() -> Config:
    """Provide a test configuration."""
    reset_config()
    # Will use environment variables
    config = Config()
    return config


@pytest.fixture
def mock_client():
    """Provide a mock httpx client."""
    return AsyncMock()


@pytest.fixture
def request_queue(logger):
    """Provide a request queue instance."""
    return RequestQueue(max_size=100, logger=logger)


@pytest.fixture
def load_balancer(logger):
    """Provide a load balancer instance."""
    return LoadBalancer(logger=logger)


@pytest.fixture
def worker_metrics():
    """Provide worker metrics instance."""
    return WorkerMetrics(
        total_requests=100,
        successful_requests=95,
        failed_requests=5,
    )
