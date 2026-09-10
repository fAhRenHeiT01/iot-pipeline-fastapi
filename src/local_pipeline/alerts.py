"""Alert Evaluation Engine for Local Gold Lakehouse Layer.

Re-exports core alert evaluation logic from pipeline.alerts for local execution.
Evaluates critical fleet telemetry alerts against a 10-minute sliding event window:
1. EXCESSIVE_IDLE: Engine on & speed 0 continuously exceeding 10 minutes.
2. OVERHEATING_CRITICAL: Engine temperature >= 75°C continuously for > 60 seconds.
3. HARSH_BRAKING: Consecutive event differential Δspeed / Δt < -15 km/h/s.
4. RAPID_ACCELERATION: Consecutive event differential Δspeed / Δt > +12 km/h/s.
5. OVERSPEEDING: Sustained speed > 110 km/h for >= 5 minutes.
6. GHOST_TOWING: Engine off with speed > 5 km/h or rapid GPS coordinate translation.
7. COLD_ENGINE_HARD_ACCEL: High speed (> 60 km/h) or rapid acceleration while engine temp < 50°C.
8. STUCK_SENSOR_ANOMALY: Zero variance in GPS coordinates or temperature across >= 5 moving events.
9. THERMAL_SPIKE_AT_IDLE: Rapid temperature rise (+5°C in <= 60s or >= 0.08°C/s) while
   stationary and idling.
"""

from pipeline.alerts import (
    evaluate_vehicle_alerts,
    evaluate_vehicle_alerts_pandas,
    haversine_km,
    parse_timestamp,
)

__all__ = [
    "haversine_km",
    "parse_timestamp",
    "evaluate_vehicle_alerts",
    "evaluate_vehicle_alerts_pandas",
]
