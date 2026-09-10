"""Health check and SLA verification route."""

from datetime import datetime, timezone

from fastapi import APIRouter, Depends

from api.dependencies import DatabricksClient, get_db_client
from api.models import HealthStatusResponse

router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthStatusResponse)
def get_health(db: DatabricksClient = Depends(get_db_client)) -> HealthStatusResponse:
    """Verify system health, DB connection, and telemetry SLA latency (<120s)."""
    settings = db.settings
    threshold_seconds = settings.SLA_MAX_LATENCY_SECONDS
    now = datetime.now(timezone.utc)

    db_connected = False
    latency_seconds = None

    try:
        with db.get_connection() as cursor:
            if cursor is not None:
                db_connected = True
                gold_table = (
                    f"{settings.DATABRICKS_CATALOG}.{settings.DATABRICKS_SCHEMA}.gold_vehicle_status"
                )
                query = f"SELECT MAX(last_event_timestamp) FROM {gold_table}"
                cursor.execute(query)
                row = cursor.fetchone()
                if row and row[0]:
                    max_ts = row[0]
                    if isinstance(max_ts, str):
                        try:
                            max_ts = datetime.fromisoformat(max_ts.replace("Z", "+00:00"))
                        except ValueError:
                            max_ts = None
                    if max_ts:
                        if max_ts.tzinfo is None:
                            max_ts = max_ts.replace(tzinfo=timezone.utc)
                        latency_seconds = max(0.0, (now - max_ts).total_seconds())
            else:
                db_connected = False
                latency_seconds = None
    except Exception:
        db_connected = False

    sla_compliant = (
        db_connected and latency_seconds is not None and latency_seconds <= threshold_seconds
    )

    status = "healthy" if sla_compliant else ("degraded" if db_connected else "unhealthy")

    return HealthStatusResponse(
        status=status,
        database_connected=db_connected,
        latest_event_latency_seconds=latency_seconds,
        sla_threshold_seconds=threshold_seconds,
        sla_compliant=sla_compliant,
        checked_at=now,
    )
