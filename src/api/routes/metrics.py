"""Prometheus metrics endpoint with rich Medallion pipeline and fleet observability."""

import time
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from api.dependencies import DatabricksClient, get_db_client
from common.logger import get_logger
from local_pipeline.db import LocalPipelineDB

logger = get_logger("api-metrics")

router = APIRouter(tags=["Metrics"])

# -----------------------------------------------------------------------------
# 1. API Serving Layer Metrics
# -----------------------------------------------------------------------------
API_REQUESTS_TOTAL = Counter(
    "fleet_api_requests_total",
    "Total HTTP requests to the Fleet API",
    ["method", "endpoint", "status"],
)

API_REQUEST_DURATION_SECONDS = Histogram(
    "fleet_api_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

# -----------------------------------------------------------------------------
# 2. Database Connectivity & Freshness SLA Metrics
# -----------------------------------------------------------------------------
FLEET_DATABASE_CONNECTED = Gauge(
    "fleet_database_connected",
    "Status of database connection (1 = connected, 0 = disconnected)",
    ["backend"],
)

FLEET_SLA_LAG_SECONDS = Gauge(
    "fleet_telemetry_lag_seconds",
    "Current event time latency against real time in seconds",
)

FLEET_SLA_COMPLIANT = Gauge(
    "fleet_telemetry_sla_compliant",
    "Whether telemetry latency is within SLA threshold (1 = compliant, 0 = breached)",
)

FLEET_SLA_THRESHOLD = Gauge(
    "fleet_telemetry_sla_threshold_seconds",
    "Configured maximum acceptable latency threshold in seconds",
)

# -----------------------------------------------------------------------------
# 3. Medallion Pipeline Ingestion & Data Quality Metrics
# -----------------------------------------------------------------------------
FLEET_BRONZE_RECORDS = Gauge(
    "fleet_bronze_records_total",
    "Total raw telemetry records ingested into Bronze layer",
)

FLEET_SILVER_EVENTS = Gauge(
    "fleet_silver_events_total",
    "Total cleaned and validated events in Silver layer",
)

FLEET_SILVER_CLAMPED_SPEED = Gauge(
    "fleet_silver_clamped_speed_total",
    "Total Silver events with out-of-bounds speed clamped to [0, 160] km/h",
)

FLEET_SILVER_QUARANTINE = Gauge(
    "fleet_silver_quarantine_total",
    "Total dead-letter queue records quarantined by error reason",
    ["reason"],
)

# -----------------------------------------------------------------------------
# 4. Fleet Operational Status & Safety Alerts Metrics
# -----------------------------------------------------------------------------
FLEET_ACTIVE_VEHICLES = Gauge(
    "fleet_active_vehicles_total",
    "Total active vehicles tracked in Gold table",
)

FLEET_VEHICLES_BY_ENGINE_STATUS = Gauge(
    "fleet_vehicles_by_engine_status",
    "Count of tracked vehicles by engine status (running or off)",
    ["engine_status"],
)

FLEET_VEHICLES_MOVING = Gauge(
    "fleet_vehicles_moving_total",
    "Total active vehicles currently moving (speed > 0 km/h)",
)

FLEET_VEHICLES_STATIONARY = Gauge(
    "fleet_vehicles_stationary_total",
    "Total active vehicles currently stationary (speed = 0 km/h)",
)

FLEET_AVG_SPEED = Gauge(
    "fleet_average_speed_kph",
    "Fleet-wide average vehicle speed in km/h",
)

FLEET_AVG_TEMP = Gauge(
    "fleet_average_engine_temp_c",
    "Fleet-wide average engine temperature in degrees Celsius",
)

FLEET_ACTIVE_ALERTS = Gauge(
    "fleet_active_alerts_total",
    "Count of active safety and anomaly alerts currently firing",
    ["alert_type"],
)

FLEET_VEHICLES_WITH_ALERT = Gauge(
    "fleet_vehicles_with_alert_total",
    "Total vehicles with at least one active alert",
)

# -----------------------------------------------------------------------------
# 5. PySpark Structured Streaming Execution Observability
# -----------------------------------------------------------------------------
FLEET_STREAMING_QUERY_ACTIVE = Gauge(
    "fleet_streaming_query_active",
    "Whether streaming query has processed a micro-batch recently (1 = active, 0 = idle)",
    ["query"],
)

FLEET_STREAMING_INPUT_ROWS_PER_SEC = Gauge(
    "fleet_streaming_input_rows_per_second",
    "Real-time input row rate of the PySpark streaming query",
    ["query"],
)

FLEET_STREAMING_PROCESSED_ROWS_PER_SEC = Gauge(
    "fleet_streaming_processed_rows_per_second",
    "Real-time processing throughput of the PySpark streaming query",
    ["query"],
)

FLEET_STREAMING_LAST_BATCH_ROWS = Gauge(
    "fleet_streaming_last_batch_rows",
    "Number of input rows processed in the latest micro-batch",
    ["query"],
)

# Cache timestamp to throttle database queries on high-frequency Prometheus scrapes
_last_refresh_timestamp: float = 0.0
_REFRESH_TTL_SECONDS: float = 1.0


def refresh_fleet_metrics(db: DatabricksClient) -> None:
    """Refresh Prometheus gauges from local SQLite or Databricks lakehouse tables."""
    global _last_refresh_timestamp

    now_monotonic = time.monotonic()
    if now_monotonic - _last_refresh_timestamp < _REFRESH_TTL_SECONDS:
        return

    _last_refresh_timestamp = now_monotonic
    settings = db.settings
    threshold = settings.SLA_MAX_LATENCY_SECONDS
    FLEET_SLA_THRESHOLD.set(threshold)

    is_sqlite = settings.DATABASE_BACKEND == "sqlite" or not settings.DATABRICKS_TOKEN
    backend_name = "sqlite" if is_sqlite else "databricks"

    if is_sqlite:
        try:
            local_db = LocalPipelineDB(settings.SQLITE_DB_PATH)
            summary = local_db.get_pipeline_metrics_summary()
            FLEET_DATABASE_CONNECTED.labels(backend="sqlite").set(1)

            # 1. Medallion Table counts
            FLEET_BRONZE_RECORDS.set(summary["table_counts"].get("bronze_fleet_raw", 0))
            FLEET_SILVER_EVENTS.set(summary["table_counts"].get("silver_fleet_events", 0))
            FLEET_SILVER_CLAMPED_SPEED.set(summary.get("speed_clamped_count", 0))

            # 2. Silver DLQ quarantine reasons
            known_reasons = {
                "Corrupt JSON Payload",
                "Missing vehicle_id",
                "Unparseable timestamp",
                "Mismatched GPS coordinates",
                "Corrupted GPS coordinates",
            }
            all_reasons = known_reasons | set(summary["quarantine_reasons"].keys())
            for reason in all_reasons:
                FLEET_SILVER_QUARANTINE.labels(reason=reason).set(
                    summary["quarantine_reasons"].get(reason, 0)
                )

            # 3. Gold fleet operational stats
            FLEET_ACTIVE_VEHICLES.set(summary.get("active_vehicles_count", 0))
            FLEET_VEHICLES_MOVING.set(summary.get("moving_vehicles_count", 0))
            FLEET_VEHICLES_STATIONARY.set(summary.get("stationary_vehicles_count", 0))
            FLEET_AVG_SPEED.set(summary.get("avg_speed_kph", 0.0))
            FLEET_AVG_TEMP.set(summary.get("avg_engine_temp_c", 0.0))

            for eng_status in ("running", "off"):
                FLEET_VEHICLES_BY_ENGINE_STATUS.labels(engine_status=eng_status).set(
                    summary["engine_status_counts"].get(eng_status, 0)
                )

            # 4. Active alerts breakdown
            known_alerts = [
                "EXCESSIVE_IDLE",
                "OVERHEATING_CRITICAL",
                "HARSH_BRAKING",
                "RAPID_ACCELERATION",
                "OVERSPEEDING",
                "GHOST_TOWING",
                "COLD_ENGINE_HARD_ACCEL",
                "STUCK_SENSOR_ANOMALY",
                "THERMAL_SPIKE_AT_IDLE",
            ]
            all_alerts = set(known_alerts) | set(summary["active_alerts"].keys())
            for alert_type in all_alerts:
                FLEET_ACTIVE_ALERTS.labels(alert_type=alert_type).set(
                    summary["active_alerts"].get(alert_type, 0)
                )
            FLEET_VEHICLES_WITH_ALERT.set(summary.get("vehicles_with_alert_count", 0))

            # 5. SLA & Freshness lag
            now_utc = datetime.now(timezone.utc)
            max_ts = summary.get("latest_event_timestamp")
            if max_ts:
                if max_ts.tzinfo is None:
                    max_ts = max_ts.replace(tzinfo=timezone.utc)
                lag_seconds = max(0.0, (now_utc - max_ts).total_seconds())
                FLEET_SLA_LAG_SECONDS.set(round(lag_seconds, 1))
                FLEET_SLA_COMPLIANT.set(1 if lag_seconds <= threshold else 0)
            else:
                FLEET_SLA_LAG_SECONDS.set(0.0)
                FLEET_SLA_COMPLIANT.set(1)

            # 6. PySpark streaming queries
            stream_queries = summary.get("streaming_queries", {})
            known_queries = ["local_bronze_stream", "local_silver_stream", "local_gold_stream"]
            all_stream_names = set(known_queries) | set(stream_queries.keys())
            for q_name in all_stream_names:
                if q_name in stream_queries:
                    q_info = stream_queries[q_name]
                    updated_str = q_info.get("updated_at")
                    is_active = 0
                    if updated_str:
                        try:
                            updated_dt = datetime.fromisoformat(updated_str.replace("Z", "+00:00"))
                            if (now_utc - updated_dt).total_seconds() <= 30.0:
                                is_active = 1
                        except Exception:
                            pass
                    FLEET_STREAMING_QUERY_ACTIVE.labels(query=q_name).set(is_active)
                    FLEET_STREAMING_INPUT_ROWS_PER_SEC.labels(query=q_name).set(
                        q_info.get("input_rows_per_sec", 0.0)
                    )
                    FLEET_STREAMING_PROCESSED_ROWS_PER_SEC.labels(query=q_name).set(
                        q_info.get("processed_rows_per_sec", 0.0)
                    )
                    FLEET_STREAMING_LAST_BATCH_ROWS.labels(query=q_name).set(
                        q_info.get("num_input_rows", 0)
                    )
                else:
                    FLEET_STREAMING_QUERY_ACTIVE.labels(query=q_name).set(0)
                    FLEET_STREAMING_INPUT_ROWS_PER_SEC.labels(query=q_name).set(0.0)
                    FLEET_STREAMING_PROCESSED_ROWS_PER_SEC.labels(query=q_name).set(0.0)
                    FLEET_STREAMING_LAST_BATCH_ROWS.labels(query=q_name).set(0)

        except Exception as exc:
            logger.error("Failed to refresh metrics from local SQLite: %s", exc)
            FLEET_DATABASE_CONNECTED.labels(backend="sqlite").set(0)
    else:
        # Databricks or mock connection
        try:
            with db.get_connection() as cursor:
                if cursor is None:
                    FLEET_DATABASE_CONNECTED.labels(backend=backend_name).set(0)
                else:
                    FLEET_DATABASE_CONNECTED.labels(backend=backend_name).set(1)
                    gold_tbl = (
                        f"{settings.DATABRICKS_CATALOG}.{settings.DATABRICKS_SCHEMA}.gold_vehicle_status"
                    )
                    cursor.execute(f"SELECT COUNT(DISTINCT vehicle_id) FROM {gold_tbl}")
                    row = cursor.fetchone()
                    if row:
                        FLEET_ACTIVE_VEHICLES.set(row[0] or 0)

                    cursor.execute(f"SELECT MAX(last_event_timestamp) FROM {gold_tbl}")
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
                            lag = max(0.0, (datetime.now(timezone.utc) - max_ts).total_seconds())
                            FLEET_SLA_LAG_SECONDS.set(round(lag, 1))
                            FLEET_SLA_COMPLIANT.set(1 if lag <= threshold else 0)
        except Exception as exc:
            logger.warning("Failed to refresh metrics from Databricks: %s", exc)
            FLEET_DATABASE_CONNECTED.labels(backend=backend_name).set(0)


@router.get("/metrics")
def get_metrics(db: DatabricksClient = Depends(get_db_client)) -> Response:
    """Expose Prometheus formatted telemetry metrics with dynamic pipeline refresh."""
    refresh_fleet_metrics(db)
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
