"""Gold Layer: Micro-batch MERGE INTO current vehicle status and alert evaluation."""

from typing import Any

from common.config import get_settings
from common.logger import get_logger

logger = get_logger("pipeline-gold")


def merge_gold_microbatch(microbatch_df: Any, batch_id: int) -> None:
    """MERGE the latest vehicle states into gold_vehicle_status."""
    try:
        from delta.tables import DeltaTable
        from pyspark.sql.functions import col, current_timestamp, desc, row_number, when
        from pyspark.sql.window import Window
    except ImportError:
        logger.error("Delta Lake / PySpark is required for Gold MERGE.")
        raise

    settings = get_settings()
    gold_table_name = f"{settings.DATABRICKS_CATALOG}.{settings.DATABRICKS_SCHEMA}.gold_vehicle_status"

    if microbatch_df.rdd.isEmpty():
        return

    # Deduplicate microbatch to find newest event per vehicle
    window_spec = Window.partitionBy("vehicle_id").orderBy(desc("event_timestamp"))
    latest_events = (
        microbatch_df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    # Compute alert flags
    enriched_latest = latest_events.select(
        col("vehicle_id"),
        col("event_timestamp").alias("last_event_timestamp"),
        col("latitude"),
        col("longitude"),
        col("speed_kph").alias("current_speed_kph"),
        col("engine_temp_c"),
        col("is_idle"),
        when(col("is_idle"), 10.0).otherwise(0.0).alias("idle_duration_minutes"),
        (col("speed_clamped") | (col("speed_kph") > 120.0)).alias("has_speed_alert"),
        current_timestamp().alias("last_updated_at"),
    )

    spark = microbatch_df.sparkSession

    # Perform Delta Lake MERGE
    if DeltaTable.isDeltaTable(spark, gold_table_name):
        gold_delta = DeltaTable.forName(spark, gold_table_name)
        (
            gold_delta.alias("target")
            .merge(
                enriched_latest.alias("source"),
                "target.vehicle_id = source.vehicle_id",
            )
            .whenMatchedUpdate(
                condition="source.last_event_timestamp >= target.last_event_timestamp",
                set={
                    "last_event_timestamp": "source.last_event_timestamp",
                    "latitude": "source.latitude",
                    "longitude": "source.longitude",
                    "current_speed_kph": "source.current_speed_kph",
                    "engine_temp_c": "source.engine_temp_c",
                    "is_idle": "source.is_idle",
                    "idle_duration_minutes": "source.idle_duration_minutes",
                    "has_speed_alert": "source.has_speed_alert",
                    "last_updated_at": "source.last_updated_at",
                },
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        enriched_latest.write.format("delta").mode("overwrite").saveAsTable(gold_table_name)

    logger.info("Gold micro-batch %s merged successfully into %s", batch_id, gold_table_name)


def run_gold_pipeline(spark: Any, trigger_available_now: bool = False) -> Any:
    """Stream clean Silver events and merge into the Gold status table."""
    settings = get_settings()
    source_table = f"{settings.DATABRICKS_CATALOG}.{settings.DATABRICKS_SCHEMA}.silver_fleet_events"
    checkpoint_dir = f"{settings.CHECKPOINT_BASE_PATH}/gold"

    silver_stream = (
        spark.readStream.table(source_table)
        .withWatermark("event_timestamp", f"{settings.WATERMARK_DURATION_MINUTES} minutes")
        .dropDuplicates(["vehicle_id", "event_timestamp"])
    )

    writer = (
        silver_stream.writeStream
        .foreachBatch(merge_gold_microbatch)
        .option("checkpointLocation", checkpoint_dir)
    )

    if trigger_available_now:
        writer = writer.trigger(availableNow=True)

    query = writer.start()
    logger.info("Gold pipeline running from %s with checkpoint %s", source_table, checkpoint_dir)
    return query


def start_gold_merge(
    spark: Any, catalog: str | None = None, schema: str | None = None
) -> Any:
    """Start Gold merge stream (Milestone 2c entrypoint)."""
    return run_gold_pipeline(spark)



if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark_session = SparkSession.builder.appName("FleetTelemetryGold").getOrCreate()
    query_handle = run_gold_pipeline(spark_session)
    query_handle.awaitTermination()

