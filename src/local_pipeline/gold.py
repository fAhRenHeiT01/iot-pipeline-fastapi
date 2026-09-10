"""Local Gold Layer: PySpark Structured Streaming for vehicle state aggregation & atomic UPSERT.

Performs micro-batch windowing to isolate the newest event per vehicle, computes
idle duration and overspeed alert flags, and performs atomic UPSERT into SQLite gold_vehicle_status.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import max as spark_max

from common.config import get_settings
from common.logger import get_logger
from local_pipeline.alerts import evaluate_vehicle_alerts_pandas
from local_pipeline.db import LocalPipelineDB
from pipeline.schemas import GOLD_STATUS_SCHEMA, SILVER_CLEAN_SCHEMA

logger = get_logger("local-pipeline-gold")


def start_local_gold_stream(
    spark: SparkSession,
    db: LocalPipelineDB | None = None,
    trigger_interval: str = "2 seconds",
) -> Any:
    """Launch long-running PySpark Structured Streaming query for Gold layer.

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
    checkpoint_dir = str((repo_root / settings.LOCAL_CHECKPOINT_DIR / "gold").resolve())
    lakehouse_silver = str((repo_root / settings.LOCAL_LAKEHOUSE_DIR / "silver").resolve())

    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)

    # Read clean Silver stream from lakehouse parquet storage
    silver_stream = (
        spark.readStream.schema(SILVER_CLEAN_SCHEMA)
        .option("ignoreMissingFiles", "true")
        .parquet(lakehouse_silver)
    )

    def merge_gold_microbatch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return

        # 1. Identify active vehicles in this micro-batch
        active_vehicles = [
            r[0] for r in batch_df.select("vehicle_id").distinct().collect()
        ]
        if not active_vehicles:
            return

        # 2. Derive upper event-time boundary for sliding lookback
        max_ts_row = batch_df.select(spark_max("event_timestamp")).collect()
        max_ts = (
            max_ts_row[0][0]
            if max_ts_row and max_ts_row[0][0]
            else datetime.now(timezone.utc)
        )

        # 3. Retrieve historical 10-minute sliding window events for active vehicles from SQLite
        history_events = local_db.get_vehicles_silver_window_batch(
            active_vehicles, max_ts, lookback_seconds=600
        )

        # 4. Construct unified sliding window Spark DataFrame
        clean_batch_df = batch_df.select(*SILVER_CLEAN_SCHEMA.fieldNames())
        if history_events:
            history_df = spark.createDataFrame(history_events, schema=SILVER_CLEAN_SCHEMA)
            combined_window_df = (
                history_df.unionByName(clean_batch_df)
                .dropDuplicates(["vehicle_id", "event_timestamp"])
            )
        else:
            combined_window_df = clean_batch_df.dropDuplicates(
                ["vehicle_id", "event_timestamp"]
            )

        # 5. Distributed Grouped Map Pandas UDF execution across Spark executor cores
        gold_alerts_df = (
            combined_window_df.groupBy("vehicle_id")
            .applyInPandas(evaluate_vehicle_alerts_pandas, schema=GOLD_STATUS_SCHEMA)
        )

        rows = gold_alerts_df.collect()
        if not rows:
            return

        # 6. Atomically refresh gold rows for active vehicles in this batch
        refreshed = local_db.refresh_gold_vehicle_status_batch(active_vehicles, rows)
        logger.info(
            "[Gold Micro-batch %s] Evaluated %s active vehicles via applyInPandas "
            "(%s rows persisted to gold_vehicle_status)",
            batch_id,
            len(active_vehicles),
            refreshed,
        )



    query = (
        silver_stream.writeStream.foreachBatch(merge_gold_microbatch)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=trigger_interval)
        .queryName("local_gold_stream")
        .start()
    )

    logger.info("Local Gold stream launched with checkpoint: %s", checkpoint_dir)
    return query
