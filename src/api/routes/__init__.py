"""API router modules."""

from api.routes.fleet import router as fleet_router
from api.routes.health import router as health_router
from api.routes.metrics import router as metrics_router

__all__ = ["fleet_router", "health_router", "metrics_router"]

