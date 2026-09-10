"""PySpark StructType schemas for Bronze, Silver, and Gold Lakehouse tables."""

from typing import Any

try:
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )
except ImportError:  # pragma: no cover
    # Minimal stand-in for environments without PySpark installed
    class _MockType:
        def __init__(self, *args, **kwargs):
            pass

    class StructField:  # type: ignore
        def __init__(self, name: str, dataType: Any = None, nullable: bool = True):
            self.name = name
            self.dataType = dataType
            self.nullable = nullable

    class StructType:  # type: ignore
        def __init__(self, fields: list[Any] | None = None):
            self.fields = fields or []

        def fieldNames(self) -> list[str]:
            return [f.name for f in self.fields]

    BooleanType = _MockType  # type: ignore
    DoubleType = _MockType  # type: ignore
    IntegerType = _MockType  # type: ignore
    LongType = _MockType  # type: ignore
    StringType = _MockType  # type: ignore
    TimestampType = _MockType  # type: ignore


# Ingested JSON structure
TELEMETRY_PAYLOAD_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), True),
    StructField("event_timestamp", LongType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_kph", DoubleType(), True),
    StructField("engine_temp_c", DoubleType(), True),
    StructField("engine_status", IntegerType(), True),
    StructField("odometer_km", DoubleType(), True),
])

# Bronze: Raw Kafka Ingestion Table
BRONZE_SCHEMA = StructType([
    StructField("raw_payload", StringType(), False),
    StructField("kafka_topic", StringType(), False),
    StructField("kafka_partition", IntegerType(), False),
    StructField("kafka_offset", LongType(), False),
    StructField("kafka_timestamp", TimestampType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
])

# Silver Quarantine: Dead Letter Queue (Terminal Garbage)
SILVER_QUARANTINE_SCHEMA = StructType([
    StructField("raw_payload", StringType(), False),
    StructField("error_reason", StringType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
])

# Silver Clean: Watermarked, Deduplicated, Clamped Events
SILVER_CLEAN_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), False),
    StructField("event_timestamp", TimestampType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_kph", DoubleType(), False),
    StructField("raw_speed_kph", DoubleType(), False),
    StructField("speed_clamped", BooleanType(), False),
    StructField("engine_temp_c", DoubleType(), True),
    StructField("engine_status", IntegerType(), True),
    StructField("odometer_km", DoubleType(), True),
    StructField("ingestion_timestamp", TimestampType(), False),
])

# Gold: Real-Time Current Fleet Status with Alerts
GOLD_STATUS_SCHEMA = StructType([
    StructField("vehicle_id", StringType(), False),
    StructField("last_event_timestamp", TimestampType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("current_speed_kph", DoubleType(), False),
    StructField("engine_temp_c", DoubleType(), True),
    StructField("engine_status", IntegerType(), True),
    StructField("alert_type", StringType(), True),
    StructField("alert_details", StringType(), True),
    StructField("last_updated_at", TimestampType(), False),
])

# Table schema registry mapping Lakehouse table names to PySpark schemas
TABLE_SCHEMAS: dict[str, StructType] = {
    "bronze_fleet_raw": BRONZE_SCHEMA,
    "silver_fleet_quarantine": SILVER_QUARANTINE_SCHEMA,
    "silver_fleet_events": SILVER_CLEAN_SCHEMA,
    "gold_vehicle_status": GOLD_STATUS_SCHEMA,
}



