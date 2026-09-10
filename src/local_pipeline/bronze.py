"""Local Bronze Layer: PySpark Structured Streaming ingestion from Kafka / Generator.

Ensures Kafka offset tracking via Spark Structured Streaming Write-Ahead Log (WAL)
checkpoints, with idempotent micro-batch persistence to local SQLite bronze_fleet_raw.
"""

from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, udf
from pyspark.sql.types import StringType

from common.config import get_settings
from common.logger import get_logger
from local_pipeline.db import LocalPipelineDB
from local_pipeline.maintenance import prune_parquet_directory

logger = get_logger("local-pipeline-bronze")


def _get_generator_udf():
    """Lazy initialize TelemetryGenerator UDF for offline simulated stream."""
    from producer.generator import TelemetryGenerator

    generator = TelemetryGenerator()

    def _gen(_idx: int) -> str:
        payload_str, _ = generator.generate_event()
        return payload_str

    return udf(_gen, StringType())


def start_local_bronze_stream(
    spark: SparkSession,
    source: str = "kafka",
    db: LocalPipelineDB | None = None,
    trigger_interval: str = "2 seconds",
    starting_offsets: str = "latest",
) -> Any:
    """Launch long-running PySpark Structured Streaming query for Bronze layer.

    Args:
        spark: PySpark SparkSession.
        source: Streaming source ('kafka' or 'generator').
        db: LocalPipelineDB handle for SQLite persistence.
        trigger_interval: Micro-batch trigger interval (e.g. '2 seconds').
        starting_offsets: Starting offsets for Kafka ('latest' or 'earliest').

    Returns:
        StreamingQuery handle.
    """
    settings = get_settings()
    local_db = db or LocalPipelineDB()

    repo_root = Path(__file__).resolve().parent.parent.parent
    checkpoint_dir = str((repo_root / settings.LOCAL_CHECKPOINT_DIR / "bronze").resolve())
    lakehouse_bronze = str((repo_root / settings.LOCAL_LAKEHOUSE_DIR / "bronze").resolve())
    Path(checkpoint_dir).mkdir(parents=True, exist_ok=True)
    Path(lakehouse_bronze).mkdir(parents=True, exist_ok=True)

    if source == "generator":
        rate_df = (
            spark.readStream.format("rate")
            .option("rowsPerSecond", 10)
            .option("numPartitions", 1)
            .load()
        )
        gen_udf = _get_generator_udf()
        bronze_df = rate_df.select(
            gen_udf(col("value")).alias("raw_payload"),
            lit(settings.KAFKA_TOPIC_RAW).alias("kafka_topic"),
            lit(0).cast("int").alias("kafka_partition"),
            col("value").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("ingestion_timestamp"),
        )
    else:
        logger.info(
            "Initializing Bronze stream with Kafka source: servers=%s, topic=%s",
            settings.KAFKA_BOOTSTRAP_SERVERS,
            settings.KAFKA_TOPIC_RAW,
        )
        kafka_options = {
            "kafka.bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "subscribe": settings.KAFKA_TOPIC_RAW,
            "startingOffsets": starting_offsets,
            "maxOffsetsPerTrigger": "10000",
            "failOnDataLoss": "false",
        }

        if settings.KAFKA_USER and settings.KAFKA_PASSWORD:
            jaas_module = "org.apache.kafka.common.security.plain.PlainLoginModule"
            jaas_config = (
                f'{jaas_module} required username="{settings.KAFKA_USER}" '
                f'password="{settings.KAFKA_PASSWORD}";'
            )
            kafka_options.update(
                {
                    "kafka.security.protocol": settings.KAFKA_SECURITY_PROTOCOL,
                    "kafka.sasl.mechanism": settings.KAFKA_SASL_MECHANISM,
                    "kafka.sasl.jaas.config": jaas_config,
                }
            )

        if settings.KAFKA_CA_CERT_PATH:
            cert_path = Path(settings.KAFKA_CA_CERT_PATH).resolve()
            if cert_path.is_file():
                kafka_options.update(
                    {
                        "kafka.ssl.truststore.type": "PEM",
                        "kafka.ssl.truststore.location": str(cert_path).replace("\\", "/"),
                    }
                )

        raw_stream = spark.readStream.format("kafka").options(**kafka_options).load()
        bronze_df = raw_stream.select(
            col("value").cast("string").alias("raw_payload"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            current_timestamp().alias("ingestion_timestamp"),
        )

    def write_bronze_microbatch(batch_df: DataFrame, batch_id: int) -> None:
        """Sink micro-batch into SQLite bronze_fleet_raw and local lakehouse storage."""
        if batch_df.isEmpty():
            return

        rows = batch_df.collect()
        inserted = local_db.insert_bronze_batch(rows)

        # Append to local lakehouse Parquet directory for downstream Silver stream
        (batch_df.write.mode("append").parquet(lakehouse_bronze))
        prune_parquet_directory(lakehouse_bronze, keep_last=3)

        logger.info(
            "[Bronze Micro-batch %s] Ingested %s events (%s inserted into SQLite bronze_fleet_raw)",
            batch_id,
            len(rows),
            inserted,
        )

    query = (
        bronze_df.writeStream.foreachBatch(write_bronze_microbatch)
        .option("checkpointLocation", checkpoint_dir)
        .trigger(processingTime=trigger_interval)
        .queryName("local_bronze_stream")
        .start()
    )

    logger.info("Local Bronze stream launched with checkpoint: %s", checkpoint_dir)
    return query
