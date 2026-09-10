"""Silver Layer: Quarantine separation, watermarking, deduplication, and speed clamping.

Implements Databricks Lakehouse medallion architecture:
1. Terminal garbage quarantine separation (dead-letter queue into silver_fleet_quarantine).
2. Watermarking and late arrival filtering (WATERMARK_DURATION_MINUTES boundary).
3. Speed clamping [0.0, 160.0 km/h] and idle state computation.
4. Stateful event deduplication on [vehicle_id, event_timestamp].
5. Partition-level sorting and atomic append into silver_fleet_events Delta table.
"""

from datetime import timedelta
from typing import Any

from common.config import get_settings
from common.logger import get_logger
from pipeline.schemas import TELEMETRY_PAYLOAD_SCHEMA

logger = get_logger("pipeline-silver")


def process_silver_microbatch(
    batch_df: Any,
    batch_id: int,
    catalog: str | None = None,
    schema: str | None = None,
) -> None:
    """Process a micro-batch from Bronze: split into Quarantine (DLQ) and Clean Silver events.

    Args:
        batch_df: PySpark DataFrame micro-batch from bronze_fleet_raw.
        batch_id: Unique micro-batch epoch ID.
        catalog: Target Unity Catalog name (defaults to settings.DATABRICKS_CATALOG).
        schema: Target schema name (defaults to settings.DATABRICKS_SCHEMA).
    """
    try:
        from pyspark.sql.functions import (
            col,
            current_timestamp,
            from_json,
            to_timestamp,
            to_utc_timestamp,
            when,
        )
        from pyspark.sql.functions import max as spark_max
    except ImportError:
        logger.error("PySpark is required for silver processing.")
        raise

    if batch_df.rdd.isEmpty():
        logger.debug("[Silver Micro-batch %s] Empty batch received; skipping.", batch_id)
        return

    settings = get_settings()
    target_catalog = catalog or settings.DATABRICKS_CATALOG
    target_schema = schema or settings.DATABRICKS_SCHEMA

    # 1. Parse JSON payload and resolve timestamp in UTC
    parsed_df = batch_df.withColumn(
        "parsed", from_json(col("raw_payload"), TELEMETRY_PAYLOAD_SCHEMA)
    ).withColumn(
        "ts_converted",
        when(
            col("parsed.event_timestamp").cast("long").isNotNull(),
            to_utc_timestamp(to_timestamp(col("parsed.event_timestamp").cast("long")), "UTC"),
        ).otherwise(to_utc_timestamp(to_timestamp(col("parsed.event_timestamp")), "UTC")),
    )

    # 2. Terminal Garbage Filters (Corrupt JSON, Null vehicle_id, bad ts, corrupt/mismatched GPS)
    is_mismatched_gps = (
        (col("parsed.latitude").isNull() & col("parsed.longitude").isNotNull())
        | (col("parsed.latitude").isNotNull() & col("parsed.longitude").isNull())
    )
    is_corrupt_gps = (
        (col("parsed.latitude") < -90.0)
        | (col("parsed.latitude") > 90.0)
        | (col("parsed.longitude") < -180.0)
        | (col("parsed.longitude") > 180.0)
    )

    is_terminal_garbage = (
        col("parsed").isNull()
        | col("parsed.vehicle_id").isNull()
        | col("ts_converted").isNull()
        | is_mismatched_gps
        | is_corrupt_gps
    )

    quarantine_df = parsed_df.filter(is_terminal_garbage).select(
        col("raw_payload"),
        when(col("parsed").isNull(), "Corrupt JSON Payload")
        .when(col("parsed.vehicle_id").isNull(), "Missing vehicle_id")
        .when(col("ts_converted").isNull(), "Unparseable timestamp")
        .when(is_mismatched_gps, "Mismatched GPS coordinates")
        .when(is_corrupt_gps, "Corrupted GPS coordinates")
        .otherwise("Unknown validation error")
        .alias("error_reason"),
        current_timestamp().alias("ingestion_timestamp"),
    )

    if not quarantine_df.rdd.isEmpty():
        quarantine_table = f"{target_catalog}.{target_schema}.silver_fleet_quarantine"
        quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)
        logger.warning(
            "[Silver Micro-batch %s] Quarantined invalid events into %s",
            batch_id,
            quarantine_table,
        )

    # 3. Clean and Transform Valid Events
    valid_df = parsed_df.filter(~is_terminal_garbage)
    if valid_df.rdd.isEmpty():
        logger.info(
            "[Silver Micro-batch %s] 0 clean events remaining after quarantine filter.",
            batch_id,
        )
        return

    # 4. Filter Late Arrivals beyond Watermark Boundary
    max_ts_row = valid_df.select(spark_max("ts_converted")).collect()
    if max_ts_row and max_ts_row[0][0]:
        max_ts = max_ts_row[0][0]
        watermark_boundary = max_ts - timedelta(minutes=settings.WATERMARK_DURATION_MINUTES)
        valid_df = valid_df.filter(col("ts_converted") >= watermark_boundary)

    if valid_df.rdd.isEmpty():
        logger.info(
            "[Silver Micro-batch %s] All events discarded by watermark boundary.",
            batch_id,
        )
        return

    # 5. Speed Clamping: 0.0 to 160.0 km/h and Idle Flag Deduction
    clamped_speed = (
        when(col("parsed.speed_kph") < 0.0, 0.0)
        .when(col("parsed.speed_kph") > 160.0, 160.0)
        .otherwise(col("parsed.speed_kph"))
    )

    cleaned_df = valid_df.select(
        col("parsed.vehicle_id").alias("vehicle_id"),
        col("ts_converted").alias("event_timestamp"),
        col("parsed.latitude").alias("latitude"),
        col("parsed.longitude").alias("longitude"),
        clamped_speed.alias("speed_kph"),
        col("parsed.speed_kph").alias("raw_speed_kph"),
        (
            (col("parsed.speed_kph") < 0.0) | (col("parsed.speed_kph") > 160.0)
        ).alias("speed_clamped"),
        col("parsed.engine_temp_c").alias("engine_temp_c"),
        col("parsed.engine_status").alias("engine_status"),
        ((col("parsed.engine_status") == 1) & (clamped_speed == 0.0)).alias("is_idle"),
        col("parsed.odometer_km").alias("odometer_km"),
        current_timestamp().alias("ingestion_timestamp"),
    ).dropDuplicates(["vehicle_id", "event_timestamp"])

    clean_table = f"{target_catalog}.{target_schema}.silver_fleet_events"
    (
        cleaned_df.sortWithinPartitions("vehicle_id", "event_timestamp")
        .write.format("delta")
        .mode("append")
        .saveAsTable(clean_table)
    )

    logger.info(
        "[Silver Micro-batch %s] Successfully appended clean events to %s",
        batch_id,
        clean_table,
    )


def run_silver_pipeline(
    spark: Any,
    catalog: str | None = None,
    schema: str | None = None,
    trigger_available_now: bool = True,
    trigger_interval: str | None = None,
) -> Any:
    """Run streaming pipeline from Bronze to Silver with watermarking & deduplication.

    Args:
        spark: PySpark SparkSession.
        catalog: Target Unity Catalog name (defaults to settings.DATABRICKS_CATALOG).
        schema: Target schema name (defaults to settings.DATABRICKS_SCHEMA).
        trigger_available_now: When True, uses availableNow=True (default for serverless/jobs).
        trigger_interval: Optional processingTime trigger interval (e.g. '2 seconds').

    Returns:
        StreamingQuery handle.
    """
    settings = get_settings()
    target_catalog = catalog or settings.DATABRICKS_CATALOG
    target_schema = schema or settings.DATABRICKS_SCHEMA

    source_table = f"{target_catalog}.{target_schema}.bronze_fleet_raw"
    checkpoint_dir = f"{settings.CHECKPOINT_BASE_PATH}/silver"

    logger.info(
        "Starting Silver cleansing stream: source=%s, checkpoint=%s",
        source_table,
        checkpoint_dir,
    )

    bronze_stream = spark.readStream.table(source_table)

    def _batch_handler(batch_df: Any, batch_id: int) -> None:
        process_silver_microbatch(
            batch_df, batch_id, catalog=target_catalog, schema=target_schema
        )

    writer = (
        bronze_stream.writeStream
        .foreachBatch(_batch_handler)
        .option("checkpointLocation", checkpoint_dir)
        .queryName("silver_fleet_events_stream")
    )

    if trigger_interval:
        writer = writer.trigger(processingTime=trigger_interval)
    elif trigger_available_now:
        writer = writer.trigger(availableNow=True)

    query = writer.start()
    logger.info("Silver pipeline running from %s (query id: %s)", source_table, query.id)
    return query


def start_silver_cleansing(
    spark: Any,
    catalog: str | None = None,
    schema: str | None = None,
    trigger_available_now: bool = True,
    trigger_interval: str | None = None,
) -> tuple[Any, Any]:
    """Start Silver cleansing stream and DLQ stream (Milestone 2b entrypoint)."""
    query = run_silver_pipeline(
        spark,
        catalog=catalog,
        schema=schema,
        trigger_available_now=trigger_available_now,
        trigger_interval=trigger_interval,
    )
    return query, query


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    settings_inst = get_settings()
    spark_session = SparkSession.builder.appName("FleetTelemetrySilver").getOrCreate()
    query_handle = run_silver_pipeline(
        spark_session,
        catalog=settings_inst.DATABRICKS_CATALOG,
        schema=settings_inst.DATABRICKS_SCHEMA,
    )
    query_handle.awaitTermination()
