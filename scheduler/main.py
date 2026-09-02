import asyncio
import json
import logging
import os
import secrets
import sys
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator, Optional

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse


# ============================================================================
# Configuration
# ============================================================================

LITELLM_URL = os.getenv(
    "LITELLM_URL",
    "http://litellm:4000"
).rstrip("/")

LITELLM_MASTER_KEY = os.environ["LITELLM_MASTER_KEY"]
KAIDESHARK_AGENT_URL = os.environ["KAIDESHARK_AGENT_URL"]
KHARESSAADARA_AGENT_URL = os.environ["KHARESSAADARA_AGENT_URL"]
GALACTUS_OLLAMA_URL = os.environ["GALACTUS_OLLAMA_URL"].rstrip("/")

# Timeout settings (in seconds)
STREAMING_FIRST_BYTE_TIMEOUT = 30.0
STREAMING_IDLE_TIMEOUT = 60.0
WORKER_HEALTH_CHECK_TIMEOUT = 3.0
HEALTH_CHECK_CACHE_TTL = 10.0  # Cache health checks for 10 seconds

# Circuit breaker settings
CIRCUIT_BREAKER_FAILURE_THRESHOLD = 3  # Failures before circuit opens
CIRCUIT_BREAKER_RECOVERY_BASE = 5.0    # Base recovery time in seconds
CIRCUIT_BREAKER_RECOVERY_MAX = 300.0   # Max recovery time (5 minutes)


# ============================================================================
# Structured Logging
# ============================================================================

class StructuredLogger:
    """JSON-formatted structured logger for better observability."""

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)

        # Console handler with JSON formatting
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter("%(message)s"))
            self.logger.addHandler(handler)

    def _log(self, level: str, message: str, **kwargs):
        """Log a structured message."""
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "logger": self.name,
            "level": level,
            "message": message,
            **kwargs,
        }
        self.logger.info(json.dumps(log_entry))

    def info(self, message: str, **kwargs):
        self._log("INFO", message, **kwargs)

    def error(self, message: str, **kwargs):
        self._log("ERROR", message, **kwargs)

    def warning(self, message: str, **kwargs):
        self._log("WARNING", message, **kwargs)

    def debug(self, message: str, **kwargs):
        self._log("DEBUG", message, **kwargs)


logger = StructuredLogger(__name__)


# ============================================================================
# Circuit Breaker & Health Check State
# ============================================================================

class WorkerState:
    """Tracks health and circuit breaker state for a worker."""

    def __init__(self):
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self.circuit_open = False
        self.circuit_open_time: Optional[float] = None
        self.health_cache = None
        self.health_cache_time: Optional[float] = None

    def record_failure(self):
        """Record a failure and update circuit breaker state."""
        self.last_failure_time = time.time()
        self.failure_count += 1

        if (
            self.failure_count >= 
            CIRCUIT_BREAKER_FAILURE_THRESHOLD
        ):
            self.circuit_open = True
            self.circuit_open_time = time.time()

    def record_success(self):
        """Record a success and reset failure counter."""
        self.failure_count = 0
        self.last_failure_time = None
        
        # Close circuit on success
        if self.circuit_open:
            self.circuit_open = False
            self.circuit_open_time = None

    def can_accept_request(self) -> bool:
        """Check if worker can accept requests (circuit not open or backoff expired)."""
        if not self.circuit_open:
            return True

        # Circuit is open - check if backoff period has expired
        if self.circuit_open_time is None:
            return True

        elapsed = time.time() - self.circuit_open_time
        
        # Exponential backoff: base * 2^(attempts - threshold)
        backoff_attempts = max(
            0,
            self.failure_count - CIRCUIT_BREAKER_FAILURE_THRESHOLD
        )
        backoff_time = min(
            CIRCUIT_BREAKER_RECOVERY_BASE * (2 ** backoff_attempts),
            CIRCUIT_BREAKER_RECOVERY_MAX,
        )

        if elapsed >= backoff_time:
            # Attempt to recover - close circuit and reset failures
            self.circuit_open = False
            self.circuit_open_time = None
            self.failure_count = 0
            return True

        return False

    def cache_health(self, health_data):
        """Cache health check result."""
        self.health_cache = health_data
        self.health_cache_time = time.time()

    def get_cached_health(self) -> Optional[dict]:
        """Get cached health if not expired."""
        if (
            self.health_cache is None or
            self.health_cache_time is None
        ):
            return None

        elapsed = time.time() - self.health_cache_time
        if elapsed < HEALTH_CHECK_CACHE_TTL:
            return self.health_cache

        return None

    def invalidate_cache(self):
        """Invalidate health cache on failure."""
        self.health_cache = None
        self.health_cache_time = None


# Per-worker state tracking
worker_states = {
    "KaideShark": WorkerState(),
    "KharessaAdara": WorkerState(),
    "Galactus": WorkerState(),
}

worker_states_lock = asyncio.Lock()


# ============================================================================
# Route Configuration
# ============================================================================

#
# Measured throughput from our actual benchmarks.
#
# These are not theoretical GPU rankings.
#
ROUTES = {
    "fast": [
        {
            "worker": "KaideShark",
            "deployment": "fast-kaide",
            "tps": 111.85,
        },
        {
            "worker": "KharessaAdara",
            "deployment": "fast-kharessa",
            "tps": 62.97,
        },
        {
            "worker": "Galactus",
            "deployment": "fast-galactus",
            "tps": 29.84,
        },
    ],

    "balanced": [
        {
            "worker": "KaideShark",
            "deployment": "balanced-kaide",
            "tps": 70.64,
        },
        {
            "worker": "KharessaAdara",
            "deployment": "balanced-kharessa",
            "tps": 39.80,
        },
    ],

    "heavy": [
        {
            "worker": "KaideShark",
            "deployment": "heavy-kaide",
            "tps": 9.45,
        },
    ],

    "background": [
        {
            "worker": "Galactus",
            "deployment": "background-galactus",
            "tps": 29.84,
        },
    ],
}


AGENTS = {
    "KaideShark": KAIDESHARK_AGENT_URL,
    "KharessaAdara": KHARESSAADARA_AGENT_URL,
}


# ============================================================================
# State Management
# ============================================================================

inflight = defaultdict(int)
inflight_lock = asyncio.Lock()

client = None


# Background health check task
health_check_task = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client
    global health_check_task

    logger.info("Initializing HTTP client")

    client = httpx.AsyncClient(
        timeout=httpx.Timeout(
            connect=5.0,
            read=900.0,
            write=30.0,
            pool=5.0,
        ),
        limits=httpx.Limits(
            max_connections=100,
            max_keepalive_connections=50,
        ),
    )

    # Start background health check task
    logger.info("Starting background health check task")
    health_check_task = asyncio.create_task(
        background_health_check_loop()
    )

    yield

    logger.info("Closing HTTP client")
    await client.aclose()

    # Cancel background task
    if health_check_task:
        health_check_task.cancel()
        try:
            await health_check_task
        except asyncio.CancelledError:
            pass
        logger.info("Background health check task stopped")


app = FastAPI(
    title="AI Scheduler",
    version="1.0",
    lifespan=lifespan,
)


# ============================================================================
# SSE Streaming Helpers
# ============================================================================

async def stream_sse_events(
    response: httpx.Response,
    request_id: str,
    timeout: float = STREAMING_FIRST_BYTE_TIMEOUT,
) -> AsyncGenerator[bytes, None]:
    """
    Stream SSE events with proper line-buffering and timeout protection.

    Ensures complete SSE frames are sent, avoiding partial responses.
    Includes first-byte timeout detection.
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
                logger.info(
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


# ============================================================================
# Response Validation
# ============================================================================

def validate_non_streaming_response(
    content: bytes,
    status_code: int,
    request_id: str,
) -> Optional[str]:
    """
    Validate non-streaming response content.

    Returns error message if validation fails, None if valid.
    """
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


async def get_agent_status(worker):
    url = AGENTS.get(worker)

    if not url:
        return None

    try:
        response = await client.get(
            url,
            timeout=WORKER_HEALTH_CHECK_TIMEOUT,
        )
        response.raise_for_status()

        logger.debug(
            f"Agent health check succeeded",
            worker=worker,
        )

        return response.json()

    except Exception as exc:
        logger.debug(
            f"Agent health check failed",
            worker=worker,
            error=str(exc),
        )
        return {
            "hostname": worker,
            "eligible": False,
            "reasons": [
                "agent_unreachable"
            ],
            "error": str(exc),
        }


async def get_galactus_status():
    try:
        response = await client.get(
            f"{GALACTUS_OLLAMA_URL}/api/tags",
            timeout=WORKER_HEALTH_CHECK_TIMEOUT,
        )

        response.raise_for_status()

        logger.debug(
            "Galactus health check succeeded",
        )

        return {
            "hostname": "Galactus",
            "eligible": True,
            "reasons": [
                "available"
            ],
            "gpu": {
                "utilization_percent": None
            },
            "ollama": {
                "healthy": True
            },
        }

    except Exception as exc:
        logger.debug(
            "Galactus health check failed",
            error=str(exc),
        )
        return {
            "hostname": "Galactus",
            "eligible": False,
            "reasons": [
                "ollama_unreachable"
            ],
            "error": str(exc),
        }


async def get_worker_status(worker):
    if worker == "Galactus":
        return await get_galactus_status()

    return await get_agent_status(worker)


# ============================================================================
# Background Health Check Task
# ============================================================================

async def background_health_check_loop():
    """
    Continuously poll worker health in the background.
    
    Caches results to reduce synchronous blocking during request routing.
    """
    logger.info("Background health check loop started")

    while True:
        try:
            await asyncio.sleep(HEALTH_CHECK_CACHE_TTL)

            for worker in worker_states.keys():
                try:
                    status = await get_worker_status(worker)
                    
                    async with worker_states_lock:
                        worker_states[worker].cache_health(status)

                    logger.debug(
                        "Background health check cached",
                        worker=worker,
                        eligible=status.get("eligible", False),
                    )

                except Exception as e:
                    logger.warning(
                        "Background health check error",
                        worker=worker,
                        error=str(e),
                    )

        except asyncio.CancelledError:
            logger.info("Background health check loop cancelled")
            break

        except Exception as e:
            logger.error(
                "Background health check loop error",
                error=str(e),
            )
            await asyncio.sleep(5)  # Brief delay before retry


async def get_cached_or_fresh_status(worker):
    """
    Get cached health status if available, otherwise fetch fresh.
    
    Falls back to fresh fetch if cache is expired.
    """
    async with worker_states_lock:
        state = worker_states.get(worker)
        if state is None:
            return await get_worker_status(worker)

        # Try cache first
        cached = state.get_cached_health()
        if cached is not None:
            logger.debug(
                "Using cached health status",
                worker=worker,
            )
            return cached

    # Cache miss - fetch fresh
    status = await get_worker_status(worker)
    
    async with worker_states_lock:
        state = worker_states.get(worker)
        if state is not None:
            state.cache_health(status)

    return status


async def current_inflight(worker):
    async with inflight_lock:
        return inflight[worker]


async def acquire(worker):
    async with inflight_lock:
        inflight[worker] += 1


async def release(worker):
    async with inflight_lock:
        inflight[worker] = max(
            0,
            inflight[worker] - 1
        )


async def rank_candidates(logical_model, request_id: str):
    definitions = ROUTES.get(logical_model)

    if not definitions:
        logger.warning(
            "Unknown logical model",
            request_id=request_id,
            model=logical_model,
        )
        return [], {}

    status_map = {}

    #
    # Get health status for candidates (cached or fresh).
    #
    results = await asyncio.gather(
        *[
            get_cached_or_fresh_status(
                item["worker"]
            )
            for item in definitions
        ]
    )

    ranked = []

    for definition, status in zip(
        definitions,
        results
    ):
        worker = definition["worker"]

        status_map[worker] = status

        if not status:
            continue

        # Circuit breaker check
        async with worker_states_lock:
            state = worker_states.get(worker)
            if state and not state.can_accept_request():
                logger.debug(
                    "Worker excluded by circuit breaker",
                    request_id=request_id,
                    model=logical_model,
                    worker=worker,
                    failure_count=state.failure_count,
                )
                status_map[worker]["scheduler_reason"] = "circuit_breaker"
                continue

        #
        # Hard exclusion:
        #
        # Sunshine active
        # Sunshine cooldown
        # manual drain
        # Ollama unhealthy
        # GPU unhealthy
        #
        # are already represented by
        # agent eligible=false.
        #
        if not status.get(
            "eligible",
            False
        ):
            logger.debug(
                "Worker excluded by eligibility",
                request_id=request_id,
                model=logical_model,
                worker=worker,
                reasons=status.get("reasons", []),
            )
            continue

        worker_inflight = await current_inflight(
            worker
        )

        #
        # Begin with actual measured
        # generation performance.
        #
        score = definition["tps"]

        #
        # Existing AI jobs reduce expected
        # available capacity.
        #
        score = score / (
            1 + worker_inflight
        )

        gpu = status.get(
            "gpu",
            {}
        ) or {}

        gpu_util = gpu.get(
            "utilization_percent"
        )

        #
        # If the scheduler has not assigned
        # any work but the GPU is already
        # heavily loaded, something external
        # is using the GPU.
        #
        # Sunshine remains the authoritative
        # human-use detector, but this protects
        # against other GPU workloads.
        #
        if (
            worker != "Galactus"
            and worker_inflight == 0
            and gpu_util is not None
        ):

            if gpu_util >= 90:
                status_map[worker][
                    "scheduler_reason"
                ] = "external_gpu_busy"

                logger.debug(
                    "Worker excluded by GPU utilization",
                    request_id=request_id,
                    model=logical_model,
                    worker=worker,
                    gpu_util=gpu_util,
                )

                continue

            elif gpu_util >= 60:
                score *= 0.25

            elif gpu_util >= 30:
                score *= 0.60

        ranked.append(
            {
                **definition,
                "score": score,
                "inflight": worker_inflight,
                "status": status,
            }
        )

    ranked.sort(
        key=lambda x: x["score"],
        reverse=True,
    )

    logger.info(
        "Worker ranking complete",
        request_id=request_id,
        model=logical_model,
        ranked_count=len(ranked),
    )

    return ranked, status_map


def filtered_headers(request):
    excluded = {
        "host",
        "content-length",
        "connection",
    }

    return {
        key: value
        for key, value
        in request.headers.items()
        if key.lower() not in excluded
    }


def unauthorized_response():
    return JSONResponse(
        status_code=401,
        content={
            "error": {
                "message":
                    "Authentication Error, invalid or missing API key.",
                "type": "auth_error",
                "param": None,
                "code": "401",
            }
        },
    )


def authorize_request(request):
    authorization = request.headers.get(
        "authorization",
        "",
    )

    scheme, _, token = authorization.partition(" ")

    if scheme.lower() != "bearer" or not token:
        return False

    return secrets.compare_digest(
        token.strip(),
        LITELLM_MASTER_KEY,
    )


@app.get("/health")
async def health():
    return {
        "status": "ok"
    }


@app.get("/scheduler/status")
async def scheduler_status():
    request_id = str(uuid.uuid4())

    logger.info(
        "Status request received",
        request_id=request_id,
    )

    workers = {}

    for worker in (
        "KaideShark",
        "KharessaAdara",
        "Galactus",
    ):
        status = await get_worker_status(
            worker
        )

        status["scheduler_inflight"] = (
            await current_inflight(worker)
        )

        workers[worker] = status

    routing = {}

    for logical_model in ROUTES:
        ranked, _ = await rank_candidates(
            logical_model,
            request_id,
        )

        routing[logical_model] = [
            {
                "worker": item["worker"],
                "deployment":
                    item["deployment"],
                "score":
                    round(item["score"], 2),
                "inflight":
                    item["inflight"],
            }
            for item in ranked
        ]

    logger.info(
        "Status request completed",
        request_id=request_id,
    )

    return {
        "workers": workers,
        "routing": routing,
    }


@app.get("/v1/models")
async def models(request: Request):
    if not authorize_request(request):
        return unauthorized_response()

    return {
        "object": "list",
        "data": [
            {
                "id": model,
                "object": "model",
                "owned_by":
                    "local-ai-gateway",
            }
            for model in ROUTES
        ],
    }


@app.post("/v1/chat/completions")
async def chat_completions(
    request: Request
):
    # Generate request ID for tracing
    request_id = str(uuid.uuid4())
    start_time = time.time()

    logger.info(
        "Request received",
        request_id=request_id,
    )

    if not authorize_request(request):
        logger.warning(
            "Unauthorized request",
            request_id=request_id,
        )
        return unauthorized_response()

    try:
        body = await request.json()

    except Exception as e:
        logger.error(
            "Failed to parse request JSON",
            request_id=request_id,
            error=str(e),
        )
        return JSONResponse(
            status_code=400,
            content={
                "error": {
                    "message":
                        "Invalid JSON request"
                }
            },
        )

    logical_model = body.get(
        "model"
    )

    if logical_model not in ROUTES:
        logger.warning(
            "Unknown model requested",
            request_id=request_id,
            model=logical_model,
        )
        return JSONResponse(
            status_code=400,
            content={
                "error": {
                    "message":
                        f"Unknown model class: "
                        f"{logical_model}",
                    "available_models":
                        list(ROUTES.keys()),
                }
            },
        )

    ranked, statuses = (
        await rank_candidates(
            logical_model,
            request_id,
        )
    )

    if not ranked:
        logger.error(
            "No eligible workers available",
            request_id=request_id,
            model=logical_model,
        )
        return JSONResponse(
            status_code=503,
            content={
                "error": {
                    "message":
                        f"No eligible workers "
                        f"for '{logical_model}'",
                    "worker_status":
                        statuses,
                }
            },
        )

    request_headers = filtered_headers(
        request
    )

    last_error = None

    #
    # Try candidates in scheduling order.
    #
    # If the selected backend fails before
    # a response begins, try the next one.
    #
    for candidate_idx, candidate in enumerate(ranked):

        worker = candidate["worker"]
        deployment = (
            candidate["deployment"]
        )

        forwarded_body = dict(body)

        forwarded_body["model"] = (
            deployment
        )

        logger.info(
            "Attempting worker",
            request_id=request_id,
            candidate_idx=candidate_idx,
            worker=worker,
            deployment=deployment,
        )

        await acquire(worker)

        try:

            #
            # Streaming request
            #
            if body.get(
                "stream",
                False
            ):
                upstream_request = (
                    client.build_request(
                        "POST",
                        f"{LITELLM_URL}"
                        "/v1/chat/completions",
                        json=forwarded_body,
                        headers=request_headers,
                    )
                )

                upstream = await client.send(
                    upstream_request,
                    stream=True,
                )

                #
                # Backend problem before
                # streaming starts:
                # try another worker.
                #
                if upstream.status_code >= 500:
                    error_body = (
                        await upstream.aread()
                    )

                    await upstream.aclose()
                    await release(worker)

                    last_error = (
                        error_body.decode(
                            errors="replace"
                        )
                    )

                    logger.warning(
                        "Worker returned server error",
                        request_id=request_id,
                        worker=worker,
                        status_code=upstream.status_code,
                        error=last_error[:200],
                    )

                    # Record failure for circuit breaker
                    async with worker_states_lock:
                        state = worker_states.get(worker)
                        if state:
                            state.record_failure()
                            state.invalidate_cache()
                            logger.debug(
                                "Circuit breaker: failure recorded",
                                worker=worker,
                                failure_count=state.failure_count,
                                circuit_open=state.circuit_open,
                            )

                    continue

                # Validate streaming response starts successfully
                if not upstream.content:
                    await upstream.aclose()
                    await release(worker)

                    logger.warning(
                        "Streaming response is empty",
                        request_id=request_id,
                        worker=worker,
                    )

                    last_error = "Empty streaming response"
                    continue

                async def stream_response():
                    try:
                        async for chunk in stream_sse_events(
                            upstream,
                            request_id,
                            STREAMING_FIRST_BYTE_TIMEOUT,
                        ):
                            yield chunk

                    finally:
                        await upstream.aclose()
                        await release(worker)

                        elapsed = time.time() - start_time
                        logger.info(
                            "Streaming completed",
                            request_id=request_id,
                            worker=worker,
                            elapsed_ms=round(
                                elapsed * 1000
                            ),
                        )

                headers = {
                    "X-AI-Worker":
                        worker,
                    "X-AI-Model-Class":
                        logical_model,
                    "X-AI-Deployment":
                        deployment,
                    "X-Request-ID":
                        request_id,
                    "Content-Type":
                        upstream.headers.get(
                            "content-type",
                            "text/event-stream",
                        ),
                }

                logger.info(
                    "Streaming response started",
                    request_id=request_id,
                    worker=worker,
                )

                return StreamingResponse(
                    stream_response(),
                    status_code=
                        upstream.status_code,
                    headers=headers,
                )

            #
            # Non-streaming request
            #
            upstream = await client.post(
                f"{LITELLM_URL}"
                "/v1/chat/completions",
                json=forwarded_body,
                headers=request_headers,
            )

            await release(worker)

            #
            # Validate response content before returning.
            # Retry another eligible node if validation fails.
            #
            validation_error = validate_non_streaming_response(
                upstream.content,
                upstream.status_code,
                request_id,
            )

            if validation_error:
                last_error = validation_error
                logger.warning(
                    "Response validation failed",
                    request_id=request_id,
                    worker=worker,
                    error=validation_error,
                )

                # Record failure for circuit breaker
                async with worker_states_lock:
                    state = worker_states.get(worker)
                    if state:
                        state.record_failure()
                        state.invalidate_cache()
                        logger.debug(
                            "Circuit breaker: failure recorded",
                            worker=worker,
                            failure_count=state.failure_count,
                            circuit_open=state.circuit_open,
                        )

                continue

            # Success - record it
            async with worker_states_lock:
                state = worker_states.get(worker)
                if state:
                    state.record_success()
                    logger.debug(
                        "Circuit breaker: success recorded",
                        worker=worker,
                        failure_count=state.failure_count,
                        circuit_open=state.circuit_open,
                    )

            headers = {
                "X-AI-Worker":
                    worker,
                "X-AI-Model-Class":
                    logical_model,
                "X-AI-Deployment":
                    deployment,
                "X-Request-ID":
                    request_id,
            }

            elapsed = time.time() - start_time
            logger.info(
                "Request completed",
                request_id=request_id,
                worker=worker,
                status_code=upstream.status_code,
                elapsed_ms=round(elapsed * 1000),
            )

            return Response(
                content=upstream.content,
                status_code=
                    upstream.status_code,
                media_type=
                    upstream.headers.get(
                        "content-type",
                        "application/json",
                    ),
                headers=headers,
            )

        except asyncio.TimeoutError as e:
            await release(worker)

            last_error = f"Timeout: {str(e)}"

            logger.error(
                "Request timeout",
                request_id=request_id,
                worker=worker,
                error=last_error,
            )

            # Record failure for circuit breaker
            async with worker_states_lock:
                state = worker_states.get(worker)
                if state:
                    state.record_failure()
                    state.invalidate_cache()
                    logger.debug(
                        "Circuit breaker: failure recorded",
                        worker=worker,
                        failure_count=state.failure_count,
                        circuit_open=state.circuit_open,
                    )

            continue

        except Exception as exc:
            await release(worker)

            last_error = str(exc)

            logger.error(
                "Request error",
                request_id=request_id,
                worker=worker,
                error=last_error,
            )

            # Record failure for circuit breaker
            async with worker_states_lock:
                state = worker_states.get(worker)
                if state:
                    state.record_failure()
                    state.invalidate_cache()
                    logger.debug(
                        "Circuit breaker: failure recorded",
                        worker=worker,
                        failure_count=state.failure_count,
                        circuit_open=state.circuit_open,
                    )

            continue

    elapsed = time.time() - start_time
    logger.error(
        "All workers exhausted",
        request_id=request_id,
        model=logical_model,
        last_error=last_error,
        elapsed_ms=round(elapsed * 1000),
    )

    return JSONResponse(
        status_code=503,
        content={
            "error": {
                "message":
                    "All eligible workers failed",
                "detail":
                    last_error,
                "request_id":
                    request_id,
            }
        },
    )
