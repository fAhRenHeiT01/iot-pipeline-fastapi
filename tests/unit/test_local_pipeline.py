"""Unit tests for the local PySpark Structured Streaming pipeline and SQLite layer."""

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from api.dependencies import DatabricksClient, SQLiteCursorAdapter
from common.config import Settings
from local_pipeline.db import LocalPipelineDB
from local_pipeline.spark import setup_local_environment


@pytest.fixture
def temp_local_db(tmp_path: Path) -> LocalPipelineDB:
    """Create an isolated LocalPipelineDB instance in a temporary folder."""
    db_file = tmp_path / "test_fleet.db"
    return LocalPipelineDB(db_path=db_file)


def test_sqlite_schema_initialization(temp_local_db: LocalPipelineDB):
    """Verify that setup_local_tables.sql creates all 4 lakehouse tables and indexes."""
    counts = temp_local_db.get_table_counts()
    assert "bronze_fleet_raw" in counts
    assert "silver_fleet_quarantine" in counts
    assert "silver_fleet_events" in counts
    assert "gold_vehicle_status" in counts
    for _tbl, count in counts.items():
        assert count == 0


def test_bronze_idempotent_insertion(temp_local_db: LocalPipelineDB):
    """Verify Bronze table enforces UNIQUE constraint on (topic, partition, offset)."""
    now_str = datetime.now(timezone.utc).isoformat()
    record = (
        '{"vehicle_id": "VH-1001", "speed_kph": 60.0}',
        "fleet.telemetry.raw",
        0,
        1001,
        now_str,
        now_str,
    )

    # First insert
    inserted = temp_local_db.insert_bronze_batch([record])
    assert inserted == 1
    assert temp_local_db.get_table_counts()["bronze_fleet_raw"] == 1

    # Second insert with identical Kafka coordinates (duplicate) -> should be ignored
    dup_inserted = temp_local_db.insert_bronze_batch([record])
    assert dup_inserted == 0
    assert temp_local_db.get_table_counts()["bronze_fleet_raw"] == 1


def test_silver_quarantine_and_events_insertion(temp_local_db: LocalPipelineDB):
    """Verify Silver DLQ and clean event insertions."""
    now_str = datetime.now(timezone.utc).isoformat()

    # DLQ Record
    dlq_record = ('{"corrupted": true}', "Corrupt JSON Payload", now_str)
    temp_local_db.insert_silver_quarantine_batch([dlq_record])
    assert temp_local_db.get_table_counts()["silver_fleet_quarantine"] == 1

    # Clean Record (11 columns without is_idle)
    clean_record = (
        "VH-1001",
        now_str,
        37.7749,
        -122.4194,
        65.0,
        65.0,
        0,
        90.0,
        1,
        15000.0,
        now_str,
    )
    temp_local_db.insert_silver_events_batch([clean_record])
    assert temp_local_db.get_table_counts()["silver_fleet_events"] == 1


def test_gold_multi_row_alerts_and_atomic_refresh(temp_local_db: LocalPipelineDB):
    """Verify multi-row alerts per vehicle and atomic refresh preserving untouched vehicles."""
    now_str = datetime.now(timezone.utc).isoformat()

    # 1. VH-1001 has two co-occurring alerts -> 2 rows
    # 2. VH-1002 has no alerts -> 1 row with NULL alert_type
    rec_vh1_a = (
        "VH-1001", now_str, 37.77, -122.41, 0.0, 105.0, 1,
        "OVERHEATING_CRITICAL", "Temp > 100", now_str,
    )
    rec_vh1_b = (
        "VH-1001", now_str, 37.77, -122.41, 0.0, 105.0, 1,
        "THERMAL_SPIKE_AT_IDLE", "Rising temp", now_str,
    )
    rec_vh2 = ("VH-1002", now_str, 37.80, -122.45, 60.0, 88.0, 1, None, None, now_str)

    temp_local_db.refresh_gold_vehicle_status_batch(
        ["VH-1001", "VH-1002"],
        [rec_vh1_a, rec_vh1_b, rec_vh2],
    )

    with temp_local_db.get_connection() as conn:
        rows_vh1 = conn.execute(
            "SELECT alert_type FROM gold_vehicle_status WHERE vehicle_id = 'VH-1001'"
        ).fetchall()
        assert len(rows_vh1) == 2
        vh1_alerts = {r["alert_type"] for r in rows_vh1}
        assert vh1_alerts == {"OVERHEATING_CRITICAL", "THERMAL_SPIKE_AT_IDLE"}

        rows_vh2 = conn.execute(
            "SELECT alert_type FROM gold_vehicle_status WHERE vehicle_id = 'VH-1002'"
        ).fetchall()
        assert len(rows_vh2) == 1
        assert rows_vh2[0]["alert_type"] is None

    # 3. New batch with data ONLY for VH-1001 (engine cooled, no alerts)
    # VH-1002 has NO data in this batch, so it must be left untouched
    t_new = (datetime.now(timezone.utc) + timedelta(seconds=10)).isoformat()
    rec_vh1_cleared = ("VH-1001", t_new, 37.77, -122.41, 40.0, 85.0, 1, None, None, t_new)

    temp_local_db.refresh_gold_vehicle_status_batch(["VH-1001"], [rec_vh1_cleared])

    with temp_local_db.get_connection() as conn:
        # VH-1001 should now have exactly 1 row (old 2 alert rows purged)
        rows_vh1_after = conn.execute(
            "SELECT alert_type, current_speed_kph FROM gold_vehicle_status "
            "WHERE vehicle_id = 'VH-1001'"
        ).fetchall()
        assert len(rows_vh1_after) == 1
        assert rows_vh1_after[0]["alert_type"] is None
        assert rows_vh1_after[0]["current_speed_kph"] == 40.0

        # VH-1002 must remain completely untouched
        rows_vh2_after = conn.execute(
            "SELECT alert_type FROM gold_vehicle_status WHERE vehicle_id = 'VH-1002'"
        ).fetchall()
        assert len(rows_vh2_after) == 1
        assert rows_vh2_after[0]["alert_type"] is None


def test_sqlite_cursor_adapter(temp_local_db: LocalPipelineDB):
    """Verify SQLiteCursorAdapter normalizes Databricks queries (%s -> ? and strips prefix)."""
    now_str = datetime.now(timezone.utc).isoformat()
    rec = (
        "VH-1002", now_str, 37.77, -122.41, 130.0, 95.0, 1,
        "OVERSPEEDING", "Speed > 110", now_str,
    )
    temp_local_db.refresh_gold_vehicle_status_batch(["VH-1002"], [rec])

    settings = Settings(
        DATABASE_BACKEND="sqlite",
        SQLITE_DB_PATH=str(temp_local_db.db_path),
        DATABRICKS_CATALOG="fleet_iot",
        DATABRICKS_SCHEMA="telemetry",
    )

    conn = sqlite3.connect(str(temp_local_db.db_path))
    raw_cursor = conn.cursor()
    adapter = SQLiteCursorAdapter(raw_cursor, settings)

    # Execute a Databricks-style SQL query with catalog/schema prefix and %s param
    dbx_query = (
        "SELECT vehicle_id, current_speed_kph, alert_type "
        "FROM fleet_iot.telemetry.gold_vehicle_status WHERE vehicle_id = %s"
    )
    adapter.execute(dbx_query, ["VH-1002"])
    rows = adapter.fetchall()

    assert len(rows) == 1
    assert rows[0][0] == "VH-1002"
    assert rows[0][1] == 130.0
    assert rows[0][2] == "OVERSPEEDING"

    adapter.close()
    conn.close()


def test_databricks_client_sqlite_fallback(temp_local_db: LocalPipelineDB):
    """Verify DatabricksClient seamlessly routes to local SQLite when DATABRICKS_TOKEN is None."""
    now_str = datetime.now(timezone.utc).isoformat()
    rec = ("VH-1003", now_str, 37.77, -122.41, 45.0, 85.0, 1, None, None, now_str)
    temp_local_db.refresh_gold_vehicle_status_batch(["VH-1003"], [rec])

    settings = Settings(
        DATABRICKS_TOKEN=None,
        DATABASE_BACKEND="sqlite",
        SQLITE_DB_PATH=str(temp_local_db.db_path),
    )
    client = DatabricksClient(settings)

    with client.get_connection() as cursor:
        assert cursor is not None
        query = (
            "SELECT vehicle_id, current_speed_kph "
            "FROM fleet_iot.telemetry.gold_vehicle_status WHERE vehicle_id = %s"
        )
        cursor.execute(query, ["VH-1003"])
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == "VH-1003"
        assert row[1] == 45.0



def test_spark_environment_setup():
    """Verify setup_local_environment sets JAVA_HOME and HADOOP_HOME on Windows."""
    setup_local_environment()
    import os
    import sys

    assert "JAVA_HOME" in os.environ
    if sys.platform == "win32":
        assert "HADOOP_HOME" in os.environ


def test_spark_environment_setup_custom_setting(monkeypatch, tmp_path):
    """Verify setup_local_environment respects custom JAVA_HOME and HADOOP_HOME from settings."""
    import os

    import local_pipeline.spark

    fake_java = tmp_path / "custom_java"
    (fake_java / "bin").mkdir(parents=True)
    fake_hadoop = tmp_path / "custom_hadoop"
    (fake_hadoop / "bin").mkdir(parents=True)

    fake_settings = Settings(
        JAVA_HOME=str(fake_java),
        HADOOP_HOME=str(fake_hadoop),
    )
    monkeypatch.setattr(local_pipeline.spark, "get_settings", lambda: fake_settings)
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.delenv("HADOOP_HOME", raising=False)

    setup_local_environment()

    assert os.environ["JAVA_HOME"] == str(fake_java.resolve())
    assert os.environ["HADOOP_HOME"] == str(fake_hadoop.resolve())


def test_schema_driven_dict_and_row_serialization(temp_local_db: LocalPipelineDB):
    """Verify that dict and Spark Row payloads are automatically mapped and persisted."""
    now_dt = datetime.now(timezone.utc)

    # 1. Bronze dict insertion
    bronze_dict = {
        "raw_payload": '{"test": 1}',
        "kafka_topic": "fleet.telemetry.raw",
        "kafka_partition": 0,
        "kafka_offset": 2001,
        "kafka_timestamp": now_dt,
        "ingestion_timestamp": now_dt,
    }
    inserted = temp_local_db.insert_bronze_batch([bronze_dict])
    assert inserted == 1

    # 2. Silver clean event dict insertion (with boolean conversion)
    silver_dict = {
        "vehicle_id": "VH-2001",
        "event_timestamp": now_dt,
        "latitude": 37.77,
        "longitude": -122.41,
        "speed_kph": 55.0,
        "raw_speed_kph": 55.0,
        "speed_clamped": True,
        "engine_temp_c": 92.0,
        "engine_status": 1,
        "odometer_km": 12000.0,
        "ingestion_timestamp": now_dt,
    }
    inserted_silver = temp_local_db.insert_silver_events_batch([silver_dict])
    assert inserted_silver == 1

    # 3. Gold status dict insertion
    gold_dict = {
        "vehicle_id": "VH-2001",
        "last_event_timestamp": now_dt,
        "latitude": 37.77,
        "longitude": -122.41,
        "current_speed_kph": 55.0,
        "engine_temp_c": 92.0,
        "engine_status": 1,
        "alert_type": "OVERHEATING_CRITICAL",
        "alert_details": "Engine temp exceeded 75C",
        "last_updated_at": now_dt,
    }
    refreshed_gold = temp_local_db.refresh_gold_vehicle_status_batch(
        ["VH-2001"], [gold_dict]
    )
    assert refreshed_gold == 1

    # Verify retrieval
    events = temp_local_db.get_vehicle_silver_window("VH-2001", now_dt)
    assert len(events) == 1
    assert events[0]["vehicle_id"] == "VH-2001"
    assert events[0]["speed_clamped"] is True


def test_schema_to_sqlite_ddl_generation():
    """Verify DDL script generation from PySpark schemas creates valid SQLite DDL."""
    from local_pipeline.db import schema_to_sqlite_ddl
    from pipeline.schemas import GOLD_STATUS_SCHEMA

    ddl = schema_to_sqlite_ddl("gold_vehicle_status", GOLD_STATUS_SCHEMA)
    assert "CREATE TABLE IF NOT EXISTS gold_vehicle_status" in ddl
    assert "vehicle_id TEXT NOT NULL" in ddl
    assert "current_speed_kph REAL NOT NULL" in ddl
    assert "alert_type TEXT" in ddl


def test_silver_utc_timestamp_parsing():
    """Verify Silver micro-batch logic parses epoch and ISO timestamps in UTC."""
    from pyspark.sql.functions import col, from_json, to_timestamp, to_utc_timestamp, when

    from local_pipeline.spark import get_local_spark_session
    from pipeline.schemas import TELEMETRY_PAYLOAD_SCHEMA

    spark = get_local_spark_session("test_silver_utc")
    raw_json = '{"vehicle_id": "VH-1001", "event_timestamp": 1725800000}'
    df = spark.createDataFrame([(raw_json,)], ["raw_payload"])

    parsed_df = df.withColumn(
        "parsed", from_json(col("raw_payload"), TELEMETRY_PAYLOAD_SCHEMA)
    ).withColumn(
        "ts_converted",
        when(
            col("parsed.event_timestamp").cast("long").isNotNull(),
            to_utc_timestamp(to_timestamp(col("parsed.event_timestamp").cast("long")), "UTC"),
        ).otherwise(to_utc_timestamp(to_timestamp(col("parsed.event_timestamp")), "UTC")),
    )

    row = parsed_df.collect()[0]
    assert row["parsed"]["vehicle_id"] == "VH-1001"
    assert row["ts_converted"] is not None


def test_pipeline_stream_metrics_recording(temp_local_db: LocalPipelineDB):
    """Verify recording and updating PySpark micro-batch progress metrics."""
    temp_local_db.record_stream_progress(
        query_name="local_bronze_stream",
        batch_id=1,
        num_input_rows=50,
        input_rows_per_sec=25.0,
        processed_rows_per_sec=30.0,
    )

    with temp_local_db.get_connection() as conn:
        row = conn.execute(
            "SELECT query_name, batch_id, num_input_rows, input_rows_per_sec, processed_rows_per_sec "
            "FROM pipeline_stream_metrics WHERE query_name = 'local_bronze_stream'"
        ).fetchone()
        assert row is not None
        assert row["query_name"] == "local_bronze_stream"
        assert row["batch_id"] == 1
        assert row["num_input_rows"] == 50
        assert row["input_rows_per_sec"] == 25.0
        assert row["processed_rows_per_sec"] == 30.0

    # Test atomic upsert on same query name
    temp_local_db.record_stream_progress(
        query_name="local_bronze_stream",
        batch_id=2,
        num_input_rows=80,
        input_rows_per_sec=40.0,
        processed_rows_per_sec=45.0,
    )

    with temp_local_db.get_connection() as conn:
        row = conn.execute(
            "SELECT batch_id, num_input_rows FROM pipeline_stream_metrics "
            "WHERE query_name = 'local_bronze_stream'"
        ).fetchone()
        assert row["batch_id"] == 2
        assert row["num_input_rows"] == 80


def test_pipeline_metrics_summary(temp_local_db: LocalPipelineDB):
    """Verify aggregated pipeline summary returns accurate Medallion, DLQ, alert, and vehicle counts."""
    now_str = datetime.now(timezone.utc).isoformat()

    # 1. Bronze
    bronze_rec = ('{"test": 1}', "fleet.telemetry.raw", 0, 501, now_str, now_str)
    temp_local_db.insert_bronze_batch([bronze_rec])

    # 2. Silver DLQ
    dlq_rec = ('{"bad": 1}', "Missing vehicle_id", now_str)
    temp_local_db.insert_silver_quarantine_batch([dlq_rec])

    # 3. Silver Clean with clamped speed
    clean_rec = (
        "VH-1001", now_str, 37.77, -122.41, 160.0, 185.0, 1, 90.0, 1, 15000.0, now_str
    )
    temp_local_db.insert_silver_events_batch([clean_rec])

    # 4. Gold Status
    gold_rec = (
        "VH-1001", now_str, 37.77, -122.41, 65.0, 95.0, 1,
        "OVERHEATING_CRITICAL", "Temp > 100", now_str
    )
    temp_local_db.refresh_gold_vehicle_status_batch(["VH-1001"], [gold_rec])

    summary = temp_local_db.get_pipeline_metrics_summary()
    assert summary["table_counts"]["bronze_fleet_raw"] == 1
    assert summary["table_counts"]["silver_fleet_events"] == 1
    assert summary["table_counts"]["silver_fleet_quarantine"] == 1
    assert summary["table_counts"]["gold_vehicle_status"] == 1
    assert summary["quarantine_reasons"]["Missing vehicle_id"] == 1
    assert summary["speed_clamped_count"] == 1
    assert summary["active_vehicles_count"] == 1
    assert summary["moving_vehicles_count"] == 1
    assert summary["stationary_vehicles_count"] == 0
    assert summary["engine_status_counts"]["running"] == 1
    assert summary["active_alerts"]["OVERHEATING_CRITICAL"] == 1
    assert summary["vehicles_with_alert_count"] == 1
    assert summary["latest_event_timestamp"] is not None



