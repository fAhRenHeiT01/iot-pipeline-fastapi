"""Streaming Driver Entrypoint for Local PySpark Structured Streaming Pipeline.

Runs long-running realtime streaming jobs writing to local SQLite database.
"""

import argparse
import os
import sys

# Ensure repo root and src are on sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
src_dir = os.path.abspath(os.path.join(current_dir, ".."))
for p in (repo_root, src_dir):
    if p not in sys.path:
        sys.path.insert(0, p)

from common.logger import get_logger  # noqa: E402
from local_pipeline.bronze import start_local_bronze_stream  # noqa: E402
from local_pipeline.db import LocalPipelineDB  # noqa: E402
from local_pipeline.gold import start_local_gold_stream  # noqa: E402
from local_pipeline.listeners import LocalPipelineStreamingListener  # noqa: E402
from local_pipeline.silver import start_local_silver_stream  # noqa: E402
from local_pipeline.spark import get_local_spark_session  # noqa: E402

logger = get_logger("local-pipeline-main")


def print_status(db: LocalPipelineDB) -> None:
    """Print current table record counts and latest timestamp."""
    counts = db.get_table_counts()
    latest_ts = db.get_latest_event_timestamp()
    print("\n" + "=" * 55)
    print("  LOCAL LAKEHOUSE DATABASE STATUS (SQLite)")
    print("=" * 55)
    print(f" Database Path: {db.db_path}")
    print("-" * 55)
    for table_name, count in counts.items():
        print(f"  {table_name.ljust(26)} : {count} records")
    print("-" * 55)
    print(f"  Latest Gold Event Timestamp: {latest_ts or 'None'}")
    print("=" * 55 + "\n")


def main() -> None:
    """Parse CLI arguments and launch local PySpark streaming pipeline."""
    parser = argparse.ArgumentParser(
        description="Local PySpark Structured Streaming Lakehouse Pipeline"
    )
    parser.add_argument(
        "--layer",
        default="all",
        choices=["bronze", "silver", "gold", "all"],
        help="Streaming layer to launch (default: all)",
    )
    parser.add_argument(
        "--source",
        default="kafka",
        choices=["kafka", "generator"],
        help="Bronze stream data source (default: kafka; use 'generator' for offline simulation)",
    )
    parser.add_argument(
        "--trigger-interval",
        default="2 seconds",
        help="Micro-batch trigger interval (default: '2 seconds')",
    )
    parser.add_argument(
        "--starting-offsets",
        default="earliest",
        choices=["earliest", "latest"],
        help="Starting offsets for Kafka stream (default: earliest)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset checkpoints, lakehouse files, and SQLite database before starting",
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Display SQLite table record counts and exit",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Custom SQLite database file path",
    )

    args = parser.parse_args()
    db = LocalPipelineDB(args.db_path)

    if args.status:
        print_status(db)
        return

    if args.reset:
        logger.info("Resetting local database, lakehouse, and checkpoints...")
        db.reset_db(clear_checkpoints=True)

    logger.info(
        "Initializing Local PySpark Session (source: %s, layer: %s)...",
        args.source,
        args.layer,
    )
    spark = get_local_spark_session(
        app_name=f"LocalLakehouseStreaming-[{args.layer}]",
        include_kafka_package=(args.source == "kafka"),
    )

    # Attach observability listener
    spark.streams.addListener(LocalPipelineStreamingListener(db=db))

    active_queries = []

    try:
        if args.layer in ("bronze", "all"):
            q_bronze = start_local_bronze_stream(
                spark,
                source=args.source,
                db=db,
                trigger_interval=args.trigger_interval,
                starting_offsets=args.starting_offsets,
            )
            active_queries.append(q_bronze)

        if args.layer in ("silver", "all"):
            q_silver = start_local_silver_stream(
                spark, db=db, trigger_interval=args.trigger_interval
            )
            active_queries.append(q_silver)

        if args.layer in ("gold", "all"):
            q_gold = start_local_gold_stream(spark, db=db, trigger_interval=args.trigger_interval)
            active_queries.append(q_gold)

        logger.info(
            "Local pipeline running in continuous realtime mode with %s active streams. "
            "Press Ctrl+C to terminate.",
            len(active_queries),
        )

        spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        logger.info("Shutdown signal received (Ctrl+C). Stopping active streaming queries...")
    finally:
        for q in active_queries:
            if q.isActive:
                q.stop()
        spark.stop()
        logger.info("Local pipeline terminated cleanly.")
        print_status(db)


if __name__ == "__main__":
    main()
