"""Bronze Layer: Ingestion from Confluent Cloud Kafka into raw append-only Delta Lake."""

from typing import Any

from common.config import get_settings
from common.logger import get_logger

logger = get_logger("pipeline-bronze")


def start_bronze_ingestion(
    spark: Any,
    catalog: str | None = None,
    schema: str | None = None,
) -> Any:
    """Stream telemetry events from Kafka into the Bronze Delta table.

    Uses availableNow=True for incremental processing on serverless compute.
    (Continuous/infinite streaming triggers like processingTime are not supported
    on free-tier / standard serverless clusters).

    Args:
        spark: PySpark SparkSession.
        catalog: Target Unity Catalog name (defaults to settings.DATABRICKS_CATALOG).
        schema: Target schema name (defaults to settings.DATABRICKS_SCHEMA).

    Returns:
        StreamingQuery handle.
    """
    try:
        from pyspark.sql.functions import col, current_timestamp
    except ImportError:
        logger.error("PySpark is required to run bronze ingestion.")
        raise

    settings = get_settings()
    target_catalog = catalog or settings.DATABRICKS_CATALOG
    target_schema = schema or settings.DATABRICKS_SCHEMA
    target_table = f"{target_catalog}.{target_schema}.bronze_fleet_raw"

    logger.info(
        "Starting Bronze ingestion stream: topic=%s, target=%s",
        settings.KAFKA_TOPIC_RAW,
        target_table,
    )

    kafka_options = {
        "kafka.bootstrap.servers": settings.KAFKA_BOOTSTRAP_SERVERS,
        "subscribe": settings.KAFKA_TOPIC_RAW,
        "startingOffsets": "latest",
        "maxOffsetsPerTrigger": "10000",
        "failOnDataLoss": "false",
    }

    if settings.KAFKA_USER and settings.KAFKA_PASSWORD:
        jaas_module = "org.apache.kafka.common.security.plain.PlainLoginModule"
        jaas_config = (
            f'{jaas_module} required username="{settings.KAFKA_USER}" '
            f'password="{settings.KAFKA_PASSWORD}";'
        )
        kafka_options.update({
            "kafka.security.protocol": settings.KAFKA_SECURITY_PROTOCOL,
            "kafka.sasl.mechanism": settings.KAFKA_SASL_MECHANISM,
            "kafka.sasl.jaas.config": jaas_config,
        })

    if settings.KAFKA_CA_CERT_PATH:
        from pathlib import Path

        cert_path = Path(settings.KAFKA_CA_CERT_PATH).resolve()
        if cert_path.is_file():
            kafka_options.update({
                "kafka.ssl.truststore.type": "PEM",
                "kafka.ssl.truststore.location": str(cert_path).replace("\\", "/"),
            })

    # Read raw Kafka stream
    raw_stream = spark.readStream.format("kafka").options(**kafka_options).load()

    bronze_df = raw_stream.select(
        col("value").cast("string").alias("raw_payload"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("ingestion_timestamp"),
    )

    checkpoint_dir = f"{settings.CHECKPOINT_BASE_PATH}/bronze"

    # Note: Infinite/continuous streaming triggers (e.g. processingTime or realTime)
    # are not supported on free-tier / standard serverless compute clusters
    # ([INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED]). We use availableNow=True to process
    # all available incoming data incrementally within serverless constraints.
    writer = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_dir)
        .trigger(availableNow=True)
        .queryName("bronze_fleet_raw_stream")
    )

    query = writer.toTable(target_table)
    logger.info("Bronze stream launched writing to %s (query id: %s)", target_table, query.id)
    return query


# Alias for backwards compatibility
run_bronze_ingestion = start_bronze_ingestion


if __name__ == "__main__":
    from pyspark.sql import SparkSession

    settings_inst = get_settings()
    spark_session = SparkSession.builder.appName("FleetTelemetryBronze").getOrCreate()
    query_handle = start_bronze_ingestion(
        spark_session,
        catalog=settings_inst.DATABRICKS_CATALOG,
        schema=settings_inst.DATABRICKS_SCHEMA,
    )
    query_handle.awaitTermination()
