"""Pydantic data models for IoT Telemetry events and fault injection."""

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field


class AnomalyType(str, Enum):
    """Data anomaly classifications matching the Data Cleaning Taxonomy."""

    CLEAN = "clean"
    TERMINAL_GARBAGE = "terminal_garbage"
    NETWORK_DUPLICATE = "network_duplicate"
    LATE_ARRIVAL = "late_arrival"
    SENSOR_NOISE = "sensor_noise"


class TelemetryPayload(BaseModel):
    """Raw IoT Telemetry Event Payload."""

    vehicle_id: str | None = Field(
        default=None, description="Unique vehicle VIN/ID; null represents terminal corruption"
    )
    event_timestamp: int | str = Field(
        default_factory=lambda: int(datetime.now(timezone.utc).timestamp()),
        description="Epoch timestamp in seconds from IoT device",
    )
    latitude: float | None = Field(
        default=37.7749,
        description="Vehicle latitude in degrees; null indicates GPS sensor issue, <-90 or >90 indicates faulty GPS sensor",
    )
    longitude: float | None = Field(
        default=-122.4194,
        description="Vehicle longitude in degrees; null indicates GPS sensor issue, <-180 or >180 indicates faulty GPS sensor",
    )
    speed_kph: float = Field(
        default=0.0, description="Vehicle speed in km/h. Values <0 or >160 represent sensor noise"
    )
    engine_temp_c: float = Field(default=90.0, description="Engine temperature in Celsius")
    engine_status: int = Field(
        default=1, ge=0, le=1, description="Engine status: 0 for off, 1 for on"
    )
    odometer_km: float = Field(default=0.0, ge=0.0)

