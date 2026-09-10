"""Silver Layer: Quarantine separation, watermarking, deduplication, and speed clamping."""

from typing import Any

from common.config import get_settings
from common.logger import get_logger
from pipeline.schemas import TELEMETRY_PAYLOAD_SCHEMA

logger = get_logger("pipeline-silver")


def process_silver_microbatch(batch_df: Any, batch_id: int) -> None:
    """Process a micro-batch from Bronze: split into Quarantine (DLQ) and Clean events."""
    try:
        from pyspark.sql.functions import col, current_timestamp, from_json, to_timestamp, when
    except ImportError:
        logger.error("PySpark is required for silver processing.")
        raise

    settings = get_settings()
    catalog = settings.DATABRICKS_CATALOG
    schema = settings.DATABRICKS_SCHEMA

    # 1. Parse JSON payload
    parsed_df = batch_df.withColumn(
        "parsed", from_json(col("raw_payload"), TELEMETRY_PAYLOAD_SCHEMA)
    ).withColumn(
        "ts_converted", to_timestamp(col("parsed.event_timestamp"))
    )

    # 2. Separate Terminal Garbage (Corrupt JSON, Null vehicle_id, unparseable ts, corrupt/mismatched GPS)
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

    quarantine_table = f"{catalog}.{schema}.silver_fleet_quarantine"
    quarantine_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

    # 3. Clean and Transform Valid Events
    valid_df = parsed_df.filter(~is_terminal_garbage)

    # Apply speed clamping: 0.0 to 160.0 km/h
    clamped_speed = (
        when(col("parsed.speed_kph") < 0.0, 0.0)
        .when(col("parsed.speed_kph") > 160.0, 160.0)
        .otherwise(col("parsed.speed_kph"))
    )

    clamped_df = valid_df.select(
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
    )

    clean_table = f"{catalog}.{schema}.silver_fleet_events"
    clamped_df.write.format("delta").mode("append").saveAsTable(clean_table)
    logger.info("Processed Silver micro-batch %s successfully.", batch_id)


def run_silver_pipeline(spark: Any, trigger_available_now: bool = False) -> Any:
    """Run streaming pipeline from Bronze to Silver with watermarking & deduplication."""
    settings = get_settings()
    source_table = f"{settings.DATABRICKS_CATALOG}.{settings.DATABRICKS_SCHEMA}.bronze_fleet_raw"
    checkpoint_dir = f"{settings.CHECKPOINT_BASE_PATH}/silver"

    bronze_stream = spark.readStream.table(source_table)

    writer = (
        bronze_stream.writeStream
        .foreachBatch(process_silver_microbatch)
        .option("checkpointLocation", checkpoint_dir)
    )

    if trigger_available_now:
        writer = writer.trigger(availableNow=True)

    query = writer.start()
    logger.info("Silver pipeline running from %s with checkpoint %s", source_table, checkpoint_dir)
    return query


def start_silver_cleansing(
    spark: Any, catalog: str | None = None, schema: str | None = None
) -> tuple[Any, Any]:
    """Start Silver cleansing stream and DLQ stream (Milestone 2b entrypoint)."""
    query = run_silver_pipeline(spark)
    return query, query



if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark_session = SparkSession.builder.appName("FleetTelemetrySilver").getOrCreate()
    query_handle = run_silver_pipeline(spark_session)
    query_handle.awaitTermination()

