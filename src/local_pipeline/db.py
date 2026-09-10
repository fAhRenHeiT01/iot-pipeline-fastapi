"""SQLite database manager and data persistence layer for local pipeline.

Provides connection management, schema initialization from setup_local_tables.sql,
and atomic micro-batch writes/upserts.
"""

import shutil
import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from common.config import get_settings
from common.logger import get_logger
from pipeline.schemas import (
    BRONZE_SCHEMA,
    GOLD_STATUS_SCHEMA,
    SILVER_CLEAN_SCHEMA,
    SILVER_QUARANTINE_SCHEMA,
    TABLE_SCHEMAS,
    StructType,
)

logger = get_logger("local-db")


def spark_type_to_sqlite(data_type: Any) -> str:
    """Map PySpark DataType to SQLite column affinity."""
    type_name = data_type.__class__.__name__
    if type_name in ("StringType",):
        return "TEXT"
    elif type_name in ("IntegerType", "LongType", "ShortType", "ByteType", "BooleanType"):
        return "INTEGER"
    elif type_name in ("DoubleType", "FloatType", "DecimalType"):
        return "REAL"
    elif type_name in ("TimestampType", "DateType"):
        return "TEXT"
    return "TEXT"


def schema_to_sqlite_ddl(
    table_name: str,
    schema: StructType,
    constraints: list[str] | None = None,
) -> str:
    """Generate CREATE TABLE IF NOT EXISTS statement from PySpark StructType."""
    column_defs = ["    id INTEGER PRIMARY KEY AUTOINCREMENT"]
    for field in schema.fields:
        sql_type = spark_type_to_sqlite(field.dataType)
        null_clause = "" if field.nullable else " NOT NULL"
        column_defs.append(f"    {field.name} {sql_type}{null_clause}")
    if constraints:
        for c in constraints:
            column_defs.append(f"    {c}")
    defs_str = ",\n".join(column_defs)
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{defs_str}\n);"


def build_insert_sql(table_name: str, schema: StructType, ignore: bool = False) -> str:
    """Generate SQL INSERT statement from PySpark StructType schema."""
    cols = ", ".join(schema.fieldNames())
    placeholders = ", ".join("?" for _ in schema.fields)
    action = "INSERT OR IGNORE INTO" if ignore else "INSERT INTO"
    return f"{action} {table_name} ({cols}) VALUES ({placeholders})"


def serialize_row_for_sqlite(row: Any, schema: StructType) -> tuple[Any, ...]:
    """Serialize a Spark Row, dict, Pydantic model, or tuple for SQLite insertion."""
    if isinstance(row, (tuple, list)):
        return tuple(
            val.isoformat()
            if hasattr(val, "isoformat")
            else (1 if val is True else (0 if val is False else val))
            for val in row
        )

    if hasattr(row, "asDict"):
        row_dict = row.asDict(recursive=False)
    elif hasattr(row, "model_dump"):
        row_dict = row.model_dump()
    elif isinstance(row, dict):
        row_dict = row
    else:
        row_dict = dict(row)

    result = []
    for field in schema.fields:
        val = row_dict.get(field.name)
        if val is None:
            result.append(None)
        elif isinstance(val, bool):
            result.append(1 if val else 0)
        elif hasattr(val, "isoformat"):
            result.append(val.isoformat())
        else:
            result.append(val)
    return tuple(result)


# Precomputed schema-driven SQL statements
SQL_INSERT_BRONZE = build_insert_sql("bronze_fleet_raw", BRONZE_SCHEMA, ignore=True)
SQL_INSERT_SILVER_QUARANTINE = build_insert_sql(
    "silver_fleet_quarantine", SILVER_QUARANTINE_SCHEMA
)
SQL_INSERT_SILVER_EVENTS = build_insert_sql("silver_fleet_events", SILVER_CLEAN_SCHEMA)
SQL_INSERT_GOLD_STATUS = build_insert_sql("gold_vehicle_status", GOLD_STATUS_SCHEMA)

SILVER_CLEAN_COLUMNS = ", ".join(SILVER_CLEAN_SCHEMA.fieldNames())


class LocalPipelineDB:
    """Manages SQLite storage for the local streaming lakehouse using PySpark schemas."""

    def __init__(self, db_path: str | Path | None = None):
        settings = get_settings()
        self.db_path = Path(db_path or settings.SQLITE_DB_PATH).resolve()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """Provide a contextual SQLite connection with foreign keys and WAL mode."""
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @classmethod
    def generate_ddl_script(cls) -> str:
        """Generate SQLite DDL script dynamically from PySpark StructType schemas."""
        scripts = [
            schema_to_sqlite_ddl(
                "bronze_fleet_raw",
                BRONZE_SCHEMA,
                constraints=[
                    "CONSTRAINT uq_bronze_kafka UNIQUE (kafka_topic, kafka_partition, kafka_offset)"
                ],
            ),
            schema_to_sqlite_ddl("silver_fleet_quarantine", SILVER_QUARANTINE_SCHEMA),
            schema_to_sqlite_ddl("silver_fleet_events", SILVER_CLEAN_SCHEMA),
            schema_to_sqlite_ddl("gold_vehicle_status", GOLD_STATUS_SCHEMA),
            (
                "CREATE TABLE IF NOT EXISTS pipeline_stream_metrics (\n"
                "    query_name TEXT PRIMARY KEY,\n"
                "    batch_id INTEGER NOT NULL,\n"
                "    num_input_rows INTEGER NOT NULL,\n"
                "    input_rows_per_sec REAL NOT NULL,\n"
                "    processed_rows_per_sec REAL NOT NULL,\n"
                "    updated_at TEXT NOT NULL\n"
                ");"
            ),
        ]
        indexes = [
            (
                "CREATE INDEX IF NOT EXISTS idx_bronze_ingestion_ts "
                "ON bronze_fleet_raw(ingestion_timestamp);"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_silver_quarantine_ts "
                "ON silver_fleet_quarantine(ingestion_timestamp);"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_silver_events_vid_ts "
                "ON silver_fleet_events(vehicle_id, event_timestamp);"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_silver_events_ingest_ts "
                "ON silver_fleet_events(ingestion_timestamp);"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_gold_vehicle_id "
                "ON gold_vehicle_status(vehicle_id);"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_gold_alert_type "
                "ON gold_vehicle_status(alert_type);"
            ),
            (
                "CREATE INDEX IF NOT EXISTS idx_gold_last_event_ts "
                "ON gold_vehicle_status(last_event_timestamp);"
            ),
        ]
        return "\n\n".join(scripts + indexes)

    def init_db(self) -> None:
        """Execute schema initialization ensuring all tables and indexes match PySpark schemas."""
        repo_root = Path(__file__).resolve().parent.parent.parent
        sql_file = repo_root / "config" / "setup_local_tables.sql"
        if sql_file.is_file():
            with open(sql_file, encoding="utf-8") as f:
                ddl_script = f.read()
        else:
            ddl_script = self.generate_ddl_script()

        with self.get_connection() as conn:
            cursor = conn.cursor()
            gold_info = cursor.execute("PRAGMA table_info(gold_vehicle_status);").fetchall()
            if gold_info:
                col_names = [r["name"] for r in gold_info]
                if "alert_type" not in col_names:
                    logger.info("Migrating gold_vehicle_status schema (dropping outdated table)")
                    conn.execute("DROP TABLE IF EXISTS gold_vehicle_status;")

            silver_info = cursor.execute("PRAGMA table_info(silver_fleet_events);").fetchall()
            if silver_info:
                silver_cols = [r["name"] for r in silver_info]
                if "is_idle" in silver_cols:
                    logger.info("Migrating silver_fleet_events schema (dropping outdated table)")
                    conn.execute("DROP TABLE IF EXISTS silver_fleet_events;")

            conn.executescript(ddl_script)
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_stream_metrics (
                    query_name TEXT PRIMARY KEY,
                    batch_id INTEGER NOT NULL,
                    num_input_rows INTEGER NOT NULL,
                    input_rows_per_sec REAL NOT NULL,
                    processed_rows_per_sec REAL NOT NULL,
                    updated_at TEXT NOT NULL
                );
                """
            )
        logger.debug("Initialized local SQLite schema")

    def reset_db(self, clear_checkpoints: bool = True) -> None:
        """Wipe tables, checkpoints, and local lakehouse storage for a clean state."""
        with self.get_connection() as conn:
            for tbl in reversed(list(TABLE_SCHEMAS.keys())):
                conn.execute(f"DROP TABLE IF EXISTS {tbl};")
            conn.execute("DROP TABLE IF EXISTS pipeline_stream_metrics;")
        self.init_db()

        if clear_checkpoints:
            settings = get_settings()
            repo_root = Path(__file__).resolve().parent.parent.parent
            for folder in [settings.LOCAL_CHECKPOINT_DIR, settings.LOCAL_LAKEHOUSE_DIR]:
                folder_path = (repo_root / folder).resolve()
                if folder_path.is_dir():
                    shutil.rmtree(folder_path, ignore_errors=True)
                    logger.info("Cleared local directory: %s", folder_path)

        logger.info("Reset local database at %s", self.db_path)

    def insert_bronze_batch(self, rows: list[Any]) -> int:
        """Insert micro-batch into bronze_fleet_raw with idempotent UNIQUE constraint."""
        if not rows:
            return 0
        records = [serialize_row_for_sqlite(r, BRONZE_SCHEMA) for r in rows]
        with self.get_connection() as conn:
            cursor = conn.executemany(SQL_INSERT_BRONZE, records)
            return cursor.rowcount

    def insert_silver_quarantine_batch(self, rows: list[Any]) -> int:
        """Insert terminal garbage records into silver_fleet_quarantine DLQ."""
        if not rows:
            return 0
        records = [serialize_row_for_sqlite(r, SILVER_QUARANTINE_SCHEMA) for r in rows]
        with self.get_connection() as conn:
            cursor = conn.executemany(SQL_INSERT_SILVER_QUARANTINE, records)
            return cursor.rowcount

    def insert_silver_events_batch(self, rows: list[Any]) -> int:
        """Insert cleaned and clamped events into silver_fleet_events."""
        if not rows:
            return 0
        records = [serialize_row_for_sqlite(r, SILVER_CLEAN_SCHEMA) for r in rows]
        with self.get_connection() as conn:
            cursor = conn.executemany(SQL_INSERT_SILVER_EVENTS, records)
            return cursor.rowcount

    def refresh_gold_vehicle_status_batch(
        self, vehicle_ids: list[str], rows: list[Any]
    ) -> int:
        """Atomically refresh gold_vehicle_status for vehicles present in the current batch.

        Removes older alert/state rows for the updated vehicles, while leaving vehicles
        without new data untouched.
        """
        if not vehicle_ids:
            return 0

        with self.get_connection() as conn:
            # Delete existing rows only for the active vehicles in this batch
            placeholders = ",".join("?" for _ in vehicle_ids)
            conn.execute(
                f"DELETE FROM gold_vehicle_status WHERE vehicle_id IN ({placeholders})",
                vehicle_ids,
            )

            if rows:
                records = [serialize_row_for_sqlite(r, GOLD_STATUS_SCHEMA) for r in rows]
                cursor = conn.executemany(SQL_INSERT_GOLD_STATUS, records)
                return cursor.rowcount
            return 0

    def upsert_gold_status_batch(self, rows: list[Any]) -> int:
        """Convenience wrapper to atomically refresh gold rows keyed by vehicle_id."""
        if not rows:
            return 0
        vehicle_ids = []
        for r in rows:
            if isinstance(r, (tuple, list)):
                vehicle_ids.append(r[0])
            elif hasattr(r, "__getitem__"):
                vehicle_ids.append(r["vehicle_id"])
            else:
                vehicle_ids.append(r.vehicle_id)
        return self.refresh_gold_vehicle_status_batch(list(set(vehicle_ids)), rows)

    def get_vehicle_silver_window(
        self,
        vehicle_id: str,
        end_time: datetime | str,
        lookback_seconds: int = 600,
    ) -> list[dict[str, Any]]:
        """Fetch chronologically ordered events for a vehicle in the sliding window.

        Args:
            vehicle_id: Target vehicle ID.
            end_time: Upper timestamp boundary (ISO format string or datetime).
            lookback_seconds: Window duration in seconds (default: 600s = 10 mins).

        Returns:
            List of event dictionaries ordered by event_timestamp ASC.
        """
        from datetime import timedelta

        if isinstance(end_time, str):
            clean_ts = end_time.replace("Z", "+00:00")
            dt_end = datetime.fromisoformat(clean_ts)
        else:
            dt_end = end_time

        dt_start = dt_end - timedelta(seconds=lookback_seconds)
        start_iso = dt_start.isoformat()
        end_iso = dt_end.isoformat()

        query = f"""
            SELECT
                {SILVER_CLEAN_COLUMNS}
            FROM silver_fleet_events
            WHERE vehicle_id = ?
              AND event_timestamp >= ?
              AND event_timestamp <= ?
            ORDER BY event_timestamp ASC
        """
        with self.get_connection() as conn:
            rows = conn.execute(query, (vehicle_id, start_iso, end_iso)).fetchall()
            results = []
            for r in rows:
                ev = dict(r)
                ev["speed_clamped"] = bool(ev["speed_clamped"])
                if isinstance(ev["event_timestamp"], str):
                    ev["event_timestamp"] = datetime.fromisoformat(
                        ev["event_timestamp"].replace("Z", "+00:00")
                    )
                if isinstance(ev["ingestion_timestamp"], str):
                    ev["ingestion_timestamp"] = datetime.fromisoformat(
                        ev["ingestion_timestamp"].replace("Z", "+00:00")
                    )
                results.append(ev)
            return results

    def get_vehicles_silver_window_batch(
        self,
        vehicle_ids: list[str],
        end_time: datetime | str,
        lookback_seconds: int = 600,
    ) -> list[dict[str, Any]]:
        """Fetch chronologically ordered events for multiple vehicles in the sliding window.

        Args:
            vehicle_ids: List of target vehicle IDs.
            end_time: Upper timestamp boundary (ISO format string or datetime).
            lookback_seconds: Window duration in seconds (default: 600s = 10 mins).

        Returns:
            List of event dictionaries matching SILVER_CLEAN_SCHEMA field names.
        """
        if not vehicle_ids:
            return []

        from datetime import timedelta

        if isinstance(end_time, str):
            clean_ts = end_time.replace("Z", "+00:00")
            dt_end = datetime.fromisoformat(clean_ts)
        else:
            dt_end = end_time

        dt_start = dt_end - timedelta(seconds=lookback_seconds)
        start_iso = dt_start.isoformat()
        end_iso = dt_end.isoformat()

        placeholders = ",".join("?" for _ in vehicle_ids)
        query = f"""
            SELECT
                {SILVER_CLEAN_COLUMNS}
            FROM silver_fleet_events
            WHERE vehicle_id IN ({placeholders})
              AND event_timestamp >= ?
              AND event_timestamp <= ?
            ORDER BY event_timestamp ASC
        """
        params = list(vehicle_ids) + [start_iso, end_iso]
        with self.get_connection() as conn:
            rows = conn.execute(query, params).fetchall()
            results = []
            for r in rows:
                ev = dict(r)
                ev["speed_clamped"] = bool(ev["speed_clamped"])
                if isinstance(ev["event_timestamp"], str):
                    ev["event_timestamp"] = datetime.fromisoformat(
                        ev["event_timestamp"].replace("Z", "+00:00")
                    )
                if isinstance(ev["ingestion_timestamp"], str):
                    ev["ingestion_timestamp"] = datetime.fromisoformat(
                        ev["ingestion_timestamp"].replace("Z", "+00:00")
                    )
                results.append(ev)
            return results

    def get_table_counts(self) -> dict[str, int]:
        """Return record counts across all lakehouse tables."""
        tables = list(TABLE_SCHEMAS.keys())
        counts = {}
        with self.get_connection() as conn:
            for tbl in tables:
                row = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
                counts[tbl] = row[0] if row else 0
        return counts

    def get_latest_event_timestamp(self) -> datetime | None:
        """Query maximum last_event_timestamp in gold_vehicle_status."""
        with self.get_connection() as conn:
            query = "SELECT MAX(last_event_timestamp) FROM gold_vehicle_status"
            row = conn.execute(query).fetchone()
            if row and row[0]:
                raw = str(row[0])
                try:
                    return datetime.fromisoformat(raw.replace("Z", "+00:00"))
                except ValueError:
                    return None
        return None

    def record_stream_progress(
        self,
        query_name: str,
        batch_id: int,
        num_input_rows: int,
        input_rows_per_sec: float,
        processed_rows_per_sec: float,
    ) -> None:
        """Record PySpark Structured Streaming micro-batch progress metrics."""
        now_iso = datetime.now(timezone.utc).isoformat()
        sql = """
            INSERT INTO pipeline_stream_metrics (
                query_name, batch_id, num_input_rows, input_rows_per_sec, processed_rows_per_sec, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(query_name) DO UPDATE SET
                batch_id=excluded.batch_id,
                num_input_rows=excluded.num_input_rows,
                input_rows_per_sec=excluded.input_rows_per_sec,
                processed_rows_per_sec=excluded.processed_rows_per_sec,
                updated_at=excluded.updated_at
        """
        with self.get_connection() as conn:
            conn.execute(
                sql,
                (
                    query_name,
                    batch_id,
                    num_input_rows,
                    input_rows_per_sec,
                    processed_rows_per_sec,
                    now_iso,
                ),
            )

    def get_pipeline_metrics_summary(self) -> dict[str, Any]:
        """Aggregate all pipeline, data quality, fleet state, and streaming metrics."""
        summary: dict[str, Any] = {
            "table_counts": {},
            "quarantine_reasons": {},
            "speed_clamped_count": 0,
            "active_vehicles_count": 0,
            "engine_status_counts": {},
            "moving_vehicles_count": 0,
            "stationary_vehicles_count": 0,
            "avg_speed_kph": 0.0,
            "avg_engine_temp_c": 0.0,
            "active_alerts": {},
            "vehicles_with_alert_count": 0,
            "latest_event_timestamp": None,
            "streaming_queries": {},
        }
        with self.get_connection() as conn:
            # 1. Table counts
            for tbl in list(TABLE_SCHEMAS.keys()):
                row = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()
                summary["table_counts"][tbl] = row[0] if row else 0

            # 2. Silver DLQ quarantine reasons
            rows = conn.execute(
                "SELECT error_reason, COUNT(*) FROM silver_fleet_quarantine GROUP BY error_reason"
            ).fetchall()
            for r in rows:
                summary["quarantine_reasons"][r[0]] = r[1]

            # 3. Silver speed clamped count
            row = conn.execute(
                "SELECT COUNT(*) FROM silver_fleet_events WHERE speed_clamped = 1"
            ).fetchone()
            summary["speed_clamped_count"] = row[0] if row else 0

            # 4. Gold vehicle states & averages
            row = conn.execute(
                "SELECT COUNT(DISTINCT vehicle_id) FROM gold_vehicle_status"
            ).fetchone()
            summary["active_vehicles_count"] = row[0] if row else 0

            row = conn.execute(
                "SELECT COUNT(DISTINCT vehicle_id) FROM gold_vehicle_status WHERE current_speed_kph > 0"
            ).fetchone()
            summary["moving_vehicles_count"] = row[0] if row else 0

            row = conn.execute(
                "SELECT COUNT(DISTINCT vehicle_id) FROM gold_vehicle_status WHERE current_speed_kph <= 0"
            ).fetchone()
            summary["stationary_vehicles_count"] = row[0] if row else 0

            row = conn.execute(
                "SELECT AVG(current_speed_kph), AVG(engine_temp_c) FROM gold_vehicle_status"
            ).fetchone()
            if row:
                summary["avg_speed_kph"] = round(row[0] or 0.0, 1)
                summary["avg_engine_temp_c"] = round(row[1] or 0.0, 1)

            # 5. Engine status breakdown
            rows = conn.execute(
                "SELECT engine_status, COUNT(DISTINCT vehicle_id) FROM gold_vehicle_status GROUP BY engine_status"
            ).fetchall()
            for r in rows:
                status_label = "running" if r[0] == 1 else "off"
                summary["engine_status_counts"][status_label] = r[1]

            # 6. Active alerts breakdown
            rows = conn.execute(
                "SELECT alert_type, COUNT(*) FROM gold_vehicle_status WHERE alert_type IS NOT NULL GROUP BY alert_type"
            ).fetchall()
            for r in rows:
                summary["active_alerts"][r[0]] = r[1]

            row = conn.execute(
                "SELECT COUNT(DISTINCT vehicle_id) FROM gold_vehicle_status WHERE alert_type IS NOT NULL"
            ).fetchone()
            summary["vehicles_with_alert_count"] = row[0] if row else 0

            # 7. Max timestamp
            row = conn.execute(
                "SELECT MAX(last_event_timestamp) FROM gold_vehicle_status"
            ).fetchone()
            if row and row[0]:
                raw = str(row[0])
                try:
                    summary["latest_event_timestamp"] = datetime.fromisoformat(
                        raw.replace("Z", "+00:00")
                    )
                except ValueError:
                    summary["latest_event_timestamp"] = None

            # 8. Streaming metrics (if table exists)
            try:
                rows = conn.execute(
                    "SELECT query_name, batch_id, num_input_rows, input_rows_per_sec, processed_rows_per_sec, updated_at "
                    "FROM pipeline_stream_metrics"
                ).fetchall()
                for r in rows:
                    summary["streaming_queries"][r["query_name"]] = {
                        "batch_id": r["batch_id"],
                        "num_input_rows": r["num_input_rows"],
                        "input_rows_per_sec": r["input_rows_per_sec"],
                        "processed_rows_per_sec": r["processed_rows_per_sec"],
                        "updated_at": r["updated_at"],
                    }
            except Exception:
                pass

        return summary
