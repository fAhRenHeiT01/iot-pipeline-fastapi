"""Pydantic API response models."""

from datetime import datetime, timezone

from pydantic import BaseModel, Field


class VehicleStatusResponse(BaseModel):
    """Latest status for an individual fleet vehicle."""

    vehicle_id: str
    last_event_timestamp: datetime
    latitude: float | None = None
    longitude: float | None = None
    current_speed_kph: float
    engine_temp_c: float | None = None
    engine_status: int | None = None
    alert_type: str | None = None
    alert_details: str | None = None
    last_updated_at: datetime



class FleetStatusList(BaseModel):
    """List response for /fleet/status."""

    total_count: int
    active_alerts_count: int
    vehicles: list[VehicleStatusResponse]


class HealthStatusResponse(BaseModel):
    """System health check response for /health."""

    status: str = Field(description="'healthy', 'degraded', or 'unhealthy'")
    database_connected: bool
    latest_event_latency_seconds: float | None = None
    sla_threshold_seconds: int
    sla_compliant: bool
    checked_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

