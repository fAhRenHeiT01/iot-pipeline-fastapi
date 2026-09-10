"""Fleet status query route."""

from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, Query

from api.dependencies import DatabricksClient, get_db_client
from api.models import FleetStatusList, VehicleStatusResponse

router = APIRouter(prefix="/fleet", tags=["Fleet"])


@router.get("/status", response_model=FleetStatusList)
def get_fleet_status(
    vehicle_id: Optional[str] = Query(None, description="Filter by vehicle VIN/ID"),
    has_alert: Optional[bool] = Query(None, description="Filter for vehicles with active alerts"),
    alert_type: Optional[str] = Query(None, description="Filter by specific alert type (e.g. EXCESSIVE_IDLE, OVERSPEEDING)"),
    limit: int = Query(50, ge=1, le=500, description="Max records to return"),
    db: DatabricksClient = Depends(get_db_client),
) -> FleetStatusList:
    """Retrieve the latest real-time vehicle statuses from Delta Lake Gold table."""
    conditions = []
    params = []

    if vehicle_id:
        conditions.append("vehicle_id = %s")
        params.append(vehicle_id)
    if has_alert is True:
        conditions.append("alert_type IS NOT NULL")
    elif has_alert is False:
        conditions.append("alert_type IS NULL")
    if alert_type:
        conditions.append("alert_type = %s")
        params.append(alert_type)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = f"""
        SELECT
            vehicle_id, last_event_timestamp, latitude, longitude,
            current_speed_kph, engine_temp_c, engine_status,
            alert_type, alert_details, last_updated_at
        FROM {db.settings.DATABRICKS_CATALOG}.{db.settings.DATABRICKS_SCHEMA}.gold_vehicle_status
        {where_clause}
        ORDER BY last_event_timestamp DESC
        LIMIT {limit}
    """

    vehicles: list[VehicleStatusResponse] = []
    with db.get_connection() as cursor:
        if cursor is not None:
            cursor.execute(query, params)
            rows = cursor.fetchall()
            for row in rows:
                vehicles.append(
                    VehicleStatusResponse(
                        vehicle_id=row[0],
                        last_event_timestamp=row[1],
                        latitude=row[2],
                        longitude=row[3],
                        current_speed_kph=row[4],
                        engine_temp_c=row[5],
                        engine_status=row[6],
                        alert_type=row[7],
                        alert_details=row[8],
                        last_updated_at=row[9],
                    )
                )
        else:
            # Fallback mock data when running locally without Databricks connection
            now = datetime.now(timezone.utc)
            vehicles = [
                VehicleStatusResponse(
                    vehicle_id="VH-1001",
                    last_event_timestamp=now,
                    latitude=37.7749,
                    longitude=-122.4194,
                    current_speed_kph=45.2,
                    engine_temp_c=89.0,
                    engine_status=1,
                    alert_type=None,
                    alert_details=None,
                    last_updated_at=now,
                )
            ]

    active_alerts = sum(1 for v in vehicles if v.alert_type is not None)
    return FleetStatusList(
        total_count=len(vehicles),
        active_alerts_count=active_alerts,
        vehicles=vehicles,
    )


