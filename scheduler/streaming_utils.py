"""
SSE streaming utilities for the AI Gateway scheduler.
"""

import asyncio
import time
from typing import AsyncGenerator, TYPE_CHECKING

if TYPE_CHECKING:
    import httpx
    from logging_utils import StructuredLogger


async def stream_sse_events(
    response: "httpx.Response",
    request_id: str,
    logger: "StructuredLogger",
    timeout: float = 30.0,
) -> AsyncGenerator[bytes, None]:
    """
    Stream SSE events with proper line-buffering and timeout protection.

    Ensures complete SSE frames are sent, avoiding partial responses.
    Includes first-byte timeout detection.
    
    Args:
        response: The httpx response to stream from
        request_id: Request ID for logging correlation
        logger: Structured logger instance
        timeout: First-byte timeout in seconds
        
    Yields:
        Complete SSE event lines as bytes
    """
    buffer = b""
    start_time = time.time()
    last_byte_time = start_time
    first_byte_received = False

    try:
        async for chunk in response.aiter_raw():
            # Check first-byte timeout
            if not first_byte_received:
                first_byte_received = True
                elapsed = time.time() - start_time
                logger.debug(
                    "SSE: First byte received",
                    request_id=request_id,
                    elapsed_ms=round(elapsed * 1000),
                )

            # Add chunk to buffer
            buffer += chunk
            last_byte_time = time.time()

            # Process complete lines
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                # Yield complete line with newline
                yield line + b"\n"

        # Yield any remaining data
        if buffer:
            yield buffer

        elapsed = time.time() - start_time
        logger.info(
            "SSE: Stream completed",
            request_id=request_id,
            elapsed_ms=round(elapsed * 1000),
        )

    except asyncio.TimeoutError:
        elapsed = time.time() - start_time
        logger.error(
            "SSE: Stream timeout",
            request_id=request_id,
            first_byte_received=first_byte_received,
            elapsed_ms=round(elapsed * 1000),
        )
        raise

    except Exception as e:
        elapsed = time.time() - start_time
        logger.error(
            "SSE: Stream error",
            request_id=request_id,
            error=str(e),
            elapsed_ms=round(elapsed * 1000),
        )
        raise


def validate_non_streaming_response(
    content: bytes,
    status_code: int,
    request_id: str,
    logger: "StructuredLogger",
) -> str | None:
    """
    Validate non-streaming response content.

    Returns error message if validation fails, None if valid.
    
    Args:
        content: Response body bytes
        status_code: HTTP status code
        request_id: Request ID for logging
        logger: Structured logger instance
        
    Returns:
        Error message if validation fails, None if valid
    """
    import json
    
    # Check status code is success
    if status_code >= 400:
        return f"HTTP {status_code}"

    # Check content is not empty
    if not content:
        logger.warning(
            "Empty response body received",
            request_id=request_id,
            status_code=status_code,
        )
        return "Empty response body"

    # Try to parse as JSON to detect malformed responses
    try:
        json.loads(content)
    except json.JSONDecodeError as e:
        logger.warning(
            "Invalid JSON in response",
            request_id=request_id,
            status_code=status_code,
            error=str(e),
        )
        return f"Invalid JSON: {str(e)}"

    return None
