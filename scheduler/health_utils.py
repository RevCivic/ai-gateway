"""
Worker health check utilities for the AI Gateway scheduler.
"""

import asyncio
import time
from typing import Optional, Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    import httpx
    from logging_utils import StructuredLogger


WORKER_HEALTH_CHECK_TIMEOUT = 3.0


async def get_agent_status(
    client: "httpx.AsyncClient",
    worker: str,
    url: str,
    logger: "StructuredLogger",
    timeout: float = WORKER_HEALTH_CHECK_TIMEOUT,
) -> Optional[Dict[str, Any]]:
    """
    Check health status of an agent worker.
    
    Args:
        client: httpx async client
        worker: Worker name
        url: Agent URL
        logger: Structured logger
        timeout: Health check timeout in seconds
        
    Returns:
        Health status dict or None
    """
    if not url:
        return None

    try:
        response = await client.get(
            url,
            timeout=timeout,
        )
        response.raise_for_status()

        logger.debug(
            "Agent health check succeeded",
            worker=worker,
        )

        return response.json()

    except Exception as exc:
        logger.debug(
            "Agent health check failed",
            worker=worker,
            error=str(exc),
        )
        return {
            "hostname": worker,
            "eligible": False,
            "reasons": ["agent_unreachable"],
            "error": str(exc),
        }


async def get_ollama_status(
    client: "httpx.AsyncClient",
    ollama_url: str,
    logger: "StructuredLogger",
    timeout: float = WORKER_HEALTH_CHECK_TIMEOUT,
) -> Dict[str, Any]:
    """
    Check health status of Ollama worker.
    
    Args:
        client: httpx async client
        ollama_url: Ollama API base URL
        logger: Structured logger
        timeout: Health check timeout in seconds
        
    Returns:
        Health status dict
    """
    try:
        response = await client.get(
            f"{ollama_url}/api/tags",
            timeout=timeout,
        )

        response.raise_for_status()

        logger.debug(
            "Ollama health check succeeded",
        )

        return {
            "hostname": "Galactus",
            "eligible": True,
            "reasons": ["available"],
            "gpu": {
                "utilization_percent": None
            },
            "ollama": {
                "healthy": True
            },
        }

    except Exception as exc:
        logger.debug(
            "Ollama health check failed",
            error=str(exc),
        )
        return {
            "hostname": "Galactus",
            "eligible": False,
            "reasons": ["ollama_unreachable"],
            "error": str(exc),
        }


async def get_worker_status(
    client: "httpx.AsyncClient",
    worker: str,
    config: Dict[str, str],
    logger: "StructuredLogger",
) -> Optional[Dict[str, Any]]:
    """
    Get status for any worker type.
    
    Args:
        client: httpx async client
        worker: Worker name
        config: Worker configuration dict with URLs
        logger: Structured logger
        
    Returns:
        Health status dict or None
    """
    if worker == "Galactus":
        ollama_url = config.get("GALACTUS_OLLAMA_URL", "").rstrip("/")
        return await get_ollama_status(client, ollama_url, logger)

    # Agent workers
    url = config.get(f"{worker.upper()}_AGENT_URL")
    return await get_agent_status(client, worker, url, logger)
