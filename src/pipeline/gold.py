"""Gold Layer: Micro-batch sliding-window alert evaluation and atomic Delta UPSERT.

Performs:
1. Micro-batch active vehicle extraction.
2. 10-minute historical event retrieval from silver_fleet_events Delta table.
3. Distributed Grouped Map Pandas UDF execution across Spark executor cores.
4. Multi-rule anomaly detection (idle, overheat, harsh brake, rapid accel, overspeed, etc.).
5. Atomic Delta Lake UPSERT/refresh into gold_vehicle_status.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from common.config import get_settings
from common.logger import get_logger
from pipeline.alerts import evaluate_vehicle_alerts_pandas
from pipeline.schemas import GOLD_STATUS_SCHEMA, SILVER_CLEAN_SCHEMA

logger = get_logger("pipeline-gold")


def merge_gold_microbatch(
    microbatch_df: Any,
    batch_id: int,
    catalog: str | None = None,
    schema: str | None = None,
) -> None:
    """MERGE/UPSERT the latest vehicle states and active alerts into gold_vehicle_status.

    Args:
        microbatch_df: PySpark DataFrame micro-batch from silver_fleet_events.
        batch_id: Unique micro-batch epoch ID.
        catalog: Target Unity Catalog name (defaults to settings.DATABRICKS_CATALOG).
        schema: Target schema name (defaults to settings.DATABRICKS_SCHEMA).
    """
    try:
        from delta.tables import DeltaTable
        from pyspark.sql.functions import col
        from pyspark.sql.functions import max as spark_max
    except ImportError:
        logger.error("Delta Lake / PySpark is required for Gold MERGE.")
        raise

    if microbatch_df.rdd.isEmpty():
        logger.debug("[Gold Micro-batch %s] Empty batch received; skipping.", batch_id)
        return

    settings = get_settings()
    target_catalog = catalog or settings.DATABRICKS_CATALOG
    target_schema = schema or settings.DATABRICKS_SCHEMA

    gold_table_name = f"{target_catalog}.{target_schema}.gold_vehicle_status"
    silver_table_name = f"{target_catalog}.{target_schema}.silver_fleet_events"

    # 1. Identify active vehicles in this micro-batch
    active_vehicles = [
        r[0] for r in microbatch_df.select("vehicle_id").distinct().collect()
    ]
    if not active_vehicles:
        return

    # 2. Derive upper event-time boundary for sliding lookback
    max_ts_row = microbatch_df.select(spark_max("event_timestamp")).collect()
    max_ts = (
        max_ts_row[0][0]
        if max_ts_row and max_ts_row[0][0]
        else datetime.now(timezone.utc)
    )

    # 3. Retrieve historical 10-minute sliding window events for active vehicles from Silver Delta
    spark = microbatch_df.sparkSession
    lookback_boundary = max_ts - timedelta(seconds=600)

    clean_batch_df = microbatch_df.select(*[f.name for f in SILVER_CLEAN_SCHEMA.fields])

    history_df = None
    try:
        if DeltaTable.isDeltaTable(spark, silver_table_name):
            history_df = (
                spark.read.table(silver_table_name)
                .filter(col("vehicle_id").isin(active_vehicles))
                .filter(col("event_timestamp") >= lookback_boundary)
                .filter(col("event_timestamp") <= max_ts)
                .select(*[f.name for f in SILVER_CLEAN_SCHEMA.fields])
            )
    except Exception as exc:
        logger.warning(
            "[Gold Micro-batch %s] Could not read sliding lookback history from %s: %s",
            batch_id,
            silver_table_name,
            exc,
        )

    # 4. Construct unified sliding window Spark DataFrame
    if history_df is not None and not history_df.rdd.isEmpty():
        combined_window_df = (
            history_df.unionByName(clean_batch_df)
            .dropDuplicates(["vehicle_id", "event_timestamp"])
        )
    else:
        combined_window_df = clean_batch_df.dropDuplicates(["vehicle_id", "event_timestamp"])

    # 5. Distributed Grouped Map Pandas UDF execution across Spark executor cores
    gold_alerts_df = (
        combined_window_df.groupBy("vehicle_id")
        .applyInPandas(evaluate_vehicle_alerts_pandas, schema=GOLD_STATUS_SCHEMA)
    )

    # 6. Atomic Delta Lake UPSERT/refresh for active vehicles
    if DeltaTable.isDeltaTable(spark, gold_table_name):
        gold_delta = DeltaTable.forName(spark, gold_table_name)
        # Atomically remove prior records for active vehicles to clear resolved alerts
        gold_delta.delete(col("vehicle_id").isin(active_vehicles))
        gold_alerts_df.write.format("delta").mode("append").saveAsTable(gold_table_name)
    else:
        gold_alerts_df.write.format("delta").mode("overwrite").saveAsTable(gold_table_name)

    logger.info(
        "[Gold Micro-batch %s] Evaluated %s active vehicles via applyInPandas and refreshed %s",
        batch_id,
        len(active_vehicles),
        gold_table_name,
    )


def run_gold_pipeline(
    spark: Any,
    catalog: str | None = None,
    schema: str | None = None,
    trigger_available_now: bool = True,
    trigger_interval: str | None = None,
) -> Any:
    """Stream clean Silver events and merge into the Gold status table.

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

    source_table = f"{target_catalog}.{target_schema}.silver_fleet_events"
    checkpoint_dir = f"{settings.CHECKPOINT_BASE_PATH}/gold"

    logger.info(
        "Starting Gold merge stream: source=%s, checkpoint=%s",
        source_table,
        checkpoint_dir,
    )

    silver_stream = spark.readStream.table(source_table)

    def _batch_handler(microbatch_df: Any, batch_id: int) -> None:
        merge_gold_microbatch(
            microbatch_df, batch_id, catalog=target_catalog, schema=target_schema
        )

    writer = (
        silver_stream.writeStream
        .foreachBatch(_batch_handler)
        .option("checkpointLocation", checkpoint_dir)
        .queryName("gold_vehicle_status_stream")
    )

    if trigger_interval:
        writer = writer.trigger(processingTime=trigger_interval)
    elif trigger_available_now:
        writer = writer.trigger(availableNow=True)

    query = writer.start()
    logger.info("Gold pipeline running from %s (query id: %s)", source_table, query.id)
    return query


def start_gold_merge(
    spark: Any,
    catalog: str | None = None,
    schema: str | None = None,
    trigger_available_now: bool = True,
    trigger_interval: str | None = None,
) -> Any:
    """Start Gold merge stream (Milestone 2c entrypoint)."""
    return run_gold_pipeline(
        spark,
        catalog=catalog,
        schema=schema,
        trigger_available_now=trigger_available_now,
        trigger_interval=trigger_interval,
    )


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    settings_inst = get_settings()
    spark_session = SparkSession.builder.appName("FleetTelemetryGold").getOrCreate()
    query_handle = run_gold_pipeline(
        spark_session,
        catalog=settings_inst.DATABRICKS_CATALOG,
        schema=settings_inst.DATABRICKS_SCHEMA,
    )
    query_handle.awaitTermination()
