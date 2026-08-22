import asyncio
import os
from collections import defaultdict
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse


LITELLM_URL = os.getenv(
    "LITELLM_URL",
    "http://litellm:4000"
).rstrip("/")

KAIDESHARK_AGENT_URL = os.environ["KAIDESHARK_AGENT_URL"]
KHARESSAADARA_AGENT_URL = os.environ["KHARESSAADARA_AGENT_URL"]
GALACTUS_OLLAMA_URL = os.environ["GALACTUS_OLLAMA_URL"].rstrip("/")


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


inflight = defaultdict(int)
inflight_lock = asyncio.Lock()

client = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global client

    client = httpx.AsyncClient(
        timeout=httpx.Timeout(
            connect=5.0,
            read=900.0,
            write=30.0,
            pool=5.0,
        )
    )

    yield

    await client.aclose()


app = FastAPI(
    title="AI Scheduler",
    version="1.0",
    lifespan=lifespan,
)


async def get_agent_status(worker):
    url = AGENTS.get(worker)

    if not url:
        return None

    try:
        response = await client.get(
            url,
            timeout=3.0,
        )
        response.raise_for_status()

        return response.json()

    except Exception as exc:
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
            timeout=3.0,
        )

        response.raise_for_status()

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


async def rank_candidates(logical_model):
    definitions = ROUTES.get(logical_model)

    if not definitions:
        return [], {}

    status_map = {}

    #
    # Poll candidate nodes concurrently.
    #
    results = await asyncio.gather(
        *[
            get_worker_status(
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


@app.get("/health")
async def health():
    return {
        "status": "ok"
    }


@app.get("/scheduler/status")
async def scheduler_status():
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
            logical_model
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

    return {
        "workers": workers,
        "routing": routing,
    }


@app.get("/v1/models")
async def models():
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
    try:
        body = await request.json()

    except Exception:
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
            logical_model
        )
    )

    if not ranked:
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
    for candidate in ranked:

        worker = candidate["worker"]
        deployment = (
            candidate["deployment"]
        )

        forwarded_body = dict(body)

        forwarded_body["model"] = (
            deployment
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

                    continue

                async def stream_response():
                    try:
                        async for chunk in (
                            upstream.aiter_raw()
                        ):
                            yield chunk

                    finally:
                        await upstream.aclose()
                        await release(worker)

                headers = {
                    "X-AI-Worker":
                        worker,
                    "X-AI-Model-Class":
                        logical_model,
                    "X-AI-Deployment":
                        deployment,
                    "Content-Type":
                        upstream.headers.get(
                            "content-type",
                            "text/event-stream",
                        ),
                }

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
            # Retry another eligible node
            # only for server/backend errors.
            #
            if upstream.status_code >= 500:
                last_error = (
                    upstream.text
                )
                continue

            headers = {
                "X-AI-Worker":
                    worker,
                "X-AI-Model-Class":
                    logical_model,
                "X-AI-Deployment":
                    deployment,
            }

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

        except Exception as exc:
            await release(worker)

            last_error = str(exc)

            continue

    return JSONResponse(
        status_code=503,
        content={
            "error": {
                "message":
                    "All eligible workers failed",
                "detail":
                    last_error,
            }
        },
    )
