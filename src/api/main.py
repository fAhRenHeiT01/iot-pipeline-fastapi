"""FastAPI application entrypoint and route configuration."""

import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware

from api.routes import fleet_router, health_router, metrics_router
from api.routes.metrics import API_REQUEST_DURATION_SECONDS, API_REQUESTS_TOTAL
from common.config import get_settings
from common.logger import get_logger

logger = get_logger("api-main")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifespan events for startup and shutdown."""
    settings = get_settings()
    logger.info("Initializing Fleet Lakehouse API (Environment: %s)", settings.ENVIRONMENT)
    yield
    logger.info("Shutting down Fleet Lakehouse API")


app = FastAPI(
    title="Fleet Real-time Lakehouse API",
    description=(
        "FastAPI Serving Layer querying Databricks Lakehouse with SLA "
        "monitoring and real-time telemetry metrics."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def prometheus_metrics_middleware(request: Request, call_next) -> Response:
    """Track request count and latency for all incoming HTTP requests."""
    start_time = time.perf_counter()
    response = await call_next(request)
    duration = time.perf_counter() - start_time

    path = request.url.path
    API_REQUESTS_TOTAL.labels(
        method=request.method,
        endpoint=path,
        status=str(response.status_code),
    ).inc()
    API_REQUEST_DURATION_SECONDS.labels(
        method=request.method,
        endpoint=path,
    ).observe(duration)

    return response


# Include Routers
app.include_router(fleet_router)
app.include_router(health_router)
app.include_router(metrics_router)


@app.get("/")
def root() -> dict[str, str]:
    """Root metadata endpoint."""
    return {
        "service": "fleet-realtime-lakehouse-api",
        "status": "online",
        "docs": "/docs",
    }
