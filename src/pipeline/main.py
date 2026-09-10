"""Streaming Driver Entrypoint for Databricks Lakehouse Streaming Pipeline."""

import argparse
import os
import sys

# Ensure repository root and src directory are on sys.path for reliable module resolution
current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
repo_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
src_dir = os.path.abspath(os.path.join(current_dir, ".."))
for p in (repo_root, src_dir):
    if p not in sys.path:
        sys.path.insert(0, p)

from pyspark.sql import SparkSession  # noqa: E402

try:
    from pipeline.bronze import start_bronze_ingestion  # noqa: E402
    from pipeline.gold import start_gold_merge  # noqa: E402
    from pipeline.listeners import FleetPipelineListener  # noqa: E402
    from pipeline.silver import start_silver_cleansing  # noqa: E402
except ImportError:
    from src.pipeline.bronze import start_bronze_ingestion  # noqa: E402
    from src.pipeline.gold import start_gold_merge  # noqa: E402
    from src.pipeline.listeners import FleetPipelineListener  # noqa: E402
    from src.pipeline.silver import start_silver_cleansing  # noqa: E402


def main() -> None:
    """Parse arguments and orchestrate Structured Streaming pipelines."""
    parser = argparse.ArgumentParser(description="Fleet Telemetry Lakehouse Streaming Driver")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument("--schema", required=True, help="Unity Catalog schema name")
    parser.add_argument("--env", default="dev", help="Target deployment environment")
    parser.add_argument(
        "--layer",
        default="bronze",
        choices=["bronze", "silver", "gold", "all"],
        help="Streaming layer to launch (default: bronze for Milestone 2a)",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName(
        f"FleetTelemetryStreaming-[{args.env}]-[{args.layer}]"
    ).getOrCreate()

    # Register Observability Listener for SLA lag and micro-batch metrics
    spark.streams.addListener(FleetPipelineListener())

    # Start Streaming Tasks
    if args.layer in ("bronze", "all"):
        start_bronze_ingestion(spark, args.catalog, args.schema)

    if args.layer in ("silver", "all"):
        start_silver_cleansing(spark, args.catalog, args.schema)

    if args.layer in ("gold", "all"):
        start_gold_merge(spark, args.catalog, args.schema)

    # Await termination across active streams
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
