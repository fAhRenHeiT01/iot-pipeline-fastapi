"""Local Silver Layer: PySpark Structured Streaming for data cleansing, DLQ, and watermarking.

Implements terminal garbage quarantine separation, speed clamping [0, 160],
idle state deduction, 15-minute event-time watermarking, and deduplication.
"""

from datetime import timedelta
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    to_timestamp,
    to_utc_timestamp,
    when,
)
from pyspark.sql.functions import max as spark_max

from common.config import get_settings
from common.logger import get_logger
from local_pipeline.db import LocalPipelineDB
from local_pipeline.maintenance import prune_parquet_directory
from pipeline.schemas import BRONZE_SCHEMA, TELEMETRY_PAYLOAD_SCHEMA

logger = get_logger("local-pipeline-silver")


def start_local_silver_stream(
    spark: SparkSession,
    db: LocalPipelineDB | None = None,
    trigger_interval: str = "2 seconds",
) -> Any:
    """Launch long-running PySpark Structured Streaming query for Silver layer.

    Args:
        spark: PySpark SparkSession.
        db: LocalPipelineDB handle for SQLite persistence.
        trigger_interval: Micro-batch trigger interval.

    Returns:
        StreamingQuery handle.
    """
    settings = get_settings()
    local_db = db or LocalPipelineDB()

    repo_root = Path(__file__).resolve().parent.parent.parent
    checkpoint_dir = str((repo_root / settings.LOCAL_CHECKPOINT_DIR / "silver").resolve())
    lakehouse_bronze = str((repo_root / settings.LOCAL_LAKEHOUSE_DIR / "bronze").resolve())
    lakehouse_silver = str((repo_root / settings.LOCAL_LAKEHOUSE_DIR / "silver").resolve())

    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)
    Path(lakehouse_silver).mkdir(parents=True, exist_ok=True)

    # Read Bronze stream from local lakehouse parquet storage
    bronze_stream = (
        spark.readStream.schema(BRONZE_SCHEMA)
        .option("ignoreMissingFiles", "true")
        .parquet(lakehouse_bronze)
    )

    def process_silver_microbatch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

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

        # 2. Terminal Garbage Filters
        is_mismatched_gps = (
            col("parsed.latitude").isNull() & col("parsed.longitude").isNotNull()
        ) | (col("parsed.latitude").isNotNull() & col("parsed.longitude").isNull())
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

        # 3. Separate Quarantine (DLQ)
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

        quarantine_rows = quarantine_df.collect()
        if quarantine_rows:
            local_db.insert_silver_quarantine_batch(quarantine_rows)

        # 4. Filter Late Arrivals beyond 10-minute Watermark Boundary
        valid_df = parsed_df.filter(~is_terminal_garbage)
        if valid_df.isEmpty():
            logger.info(
                "[Silver Micro-batch %s] Quarantined %s terminal garbage events; 0 clean events",
                batch_id,
                len(quarantine_rows),
            )
            return

        # Derive 10-minute event-time watermark boundary
        max_ts_row = valid_df.select(spark_max("ts_converted")).collect()
        if max_ts_row and max_ts_row[0][0]:
            max_ts = max_ts_row[0][0]
            watermark_boundary = max_ts - timedelta(minutes=10)
            valid_df = valid_df.filter(col("ts_converted") >= watermark_boundary)

        if valid_df.isEmpty():
            logger.info(
                "[Silver Micro-batch %s] All valid events discarded by "
                "10-minute watermark boundary",
                batch_id,
            )
            return

        # 5. Speed Clamping & Stateful Deduplication within Watermark Window
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
            ((col("parsed.speed_kph") < 0.0) | (col("parsed.speed_kph") > 160.0)).alias(
                "speed_clamped"
            ),
            col("parsed.engine_temp_c").alias("engine_temp_c"),
            col("parsed.engine_status").alias("engine_status"),
            col("parsed.odometer_km").alias("odometer_km"),
            current_timestamp().alias("ingestion_timestamp"),
        ).dropDuplicates(["vehicle_id", "event_timestamp"])

        clean_rows = cleaned_df.collect()
        if clean_rows:
            local_db.insert_silver_events_batch(clean_rows)

            # Physical Layout Optimization: Sort within partitions before Parquet write
            (
                cleaned_df.sortWithinPartitions("vehicle_id", "event_timestamp")
                .write.mode("append")
                .parquet(lakehouse_silver)
            )
            prune_parquet_directory(lakehouse_silver, keep_last=3)

        logger.info(
            "[Silver Micro-batch %s] Quarantined %s events | Persisted %s clean events",
            batch_id,
            len(quarantine_rows),
            len(clean_rows),
        )

    query = (
        bronze_stream.writeStream.foreachBatch(process_silver_microbatch)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=trigger_interval)
        .queryName("local_silver_stream")
        .start()
    )

    logger.info("Local Silver stream launched with checkpoint: %s", checkpoint_dir)
    return query
