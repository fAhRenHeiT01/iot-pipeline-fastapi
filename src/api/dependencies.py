"""Database connection and dependency injection for Databricks SQL Serverless and Local SQLite."""

import sqlite3
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from common.config import Settings, get_settings
from common.logger import get_logger

logger = get_logger("api-dependencies")


class SQLiteCursorAdapter:
    """Wraps sqlite3 cursor to normalize Databricks SQL queries for SQLite."""

    def __init__(self, cursor: sqlite3.Cursor, settings: Settings):
        self.cursor = cursor
        self.settings = settings

    def execute(self, query: str, params: Any = None) -> Any:
        # Strip catalog and schema prefixes (e.g. fleet_iot.telemetry.gold_vehicle_status)
        prefix = f"{self.settings.DATABRICKS_CATALOG}.{self.settings.DATABRICKS_SCHEMA}."
        normalized_query = query.replace(prefix, "")
        # Replace %s with ? for SQLite parameter binding
        normalized_query = normalized_query.replace("%s", "?")

        if params is None:
            return self.cursor.execute(normalized_query)
        converted_params = [
            (1 if p is True else (0 if p is False else p)) for p in params
        ]
        return self.cursor.execute(normalized_query, converted_params)

    def fetchall(self) -> list[Any]:
        return self.cursor.fetchall()

    def fetchone(self) -> Any:
        return self.cursor.fetchone()

    def close(self) -> None:
        self.cursor.close()


class DatabricksClient:
    """Manages queries against Databricks SQL Serverless or Local SQLite."""

    def __init__(self, settings: Settings):
        self.settings = settings

    @contextmanager
    def get_connection(self) -> Generator[Any, None, None]:
        """Provide a cursor to Databricks SQL or local SQLite."""
        # 1. Use Local SQLite if configured or if no Databricks token exists
        sqlite_file = Path(self.settings.SQLITE_DB_PATH)
        use_sqlite = (
            self.settings.DATABASE_BACKEND == "sqlite"
            or (not self.settings.DATABRICKS_TOKEN and sqlite_file.is_file())
        )

        if use_sqlite and sqlite_file.is_file():
            logger.debug("Connecting to local SQLite database at %s", sqlite_file)
            conn = sqlite3.connect(str(sqlite_file.resolve()))
            try:
                cursor = conn.cursor()
                yield SQLiteCursorAdapter(cursor, self.settings)
            finally:
                cursor.close()
                conn.close()
            return

        # 2. Databricks SQL Serverless connection
        if not self.settings.DATABRICKS_TOKEN:
            logger.warning(
                "No Databricks token or local SQLite DB found; running in mock/offline mode."
            )
            yield None
            return

        try:
            from databricks import sql

            connection = sql.connect(
                server_hostname=self.settings.DATABRICKS_HOST.replace("https://", ""),
                http_path=self.settings.DATABRICKS_HTTP_PATH,
                access_token=self.settings.DATABRICKS_TOKEN,
                catalog=self.settings.DATABRICKS_CATALOG,
                schema=self.settings.DATABRICKS_SCHEMA,
            )
            try:
                cursor = connection.cursor()
                yield cursor
            finally:
                cursor.close()
                connection.close()
        except Exception as exc:
            logger.error("Failed to connect to Databricks SQL: %s", exc)
            raise


def get_db_client() -> DatabricksClient:
    """FastAPI dependency yielding DatabricksClient."""
    settings = get_settings()
    return DatabricksClient(settings)
