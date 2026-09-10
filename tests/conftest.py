"""Shared pytest fixtures, mock clients, and local test helpers."""

import json
from pathlib import Path
from typing import Any, Generator

from fastapi.testclient import TestClient
import pytest

from api.dependencies import DatabricksClient, get_db_client
from api.main import app
from common.config import Settings, get_settings


class MockDatabricksCursor:
    """Mock database cursor returning sample fleet records."""

    def execute(self, query: str, params: Any = None) -> None:
        self.query = query

    def fetchall(self) -> list[tuple[Any, ...]]:
        from datetime import datetime
        now = datetime.now()
        return [
            (
                "VH-1001", now, 37.7749, -122.4194, 52.0, 88.0,
                1, None, None, now
            ),
            (
                "VH-1002", now, 37.7833, -122.4167, 0.0, 92.0,
                1, "EXCESSIVE_IDLE", "Vehicle idling continuously for 12.0m", now
            ),
        ]

    def fetchone(self) -> tuple[Any, ...]:
        from datetime import datetime
        return (datetime.now(),)




    def close(self) -> None:
        pass


class MockDatabricksClient(DatabricksClient):
    """Mock Databricks client yielding MockDatabricksCursor."""

    def __init__(self):
        super().__init__(Settings(ENVIRONMENT="test"))

    def get_connection(self):
        from contextlib import contextmanager

        @contextmanager
        def _mock_conn():
            yield MockDatabricksCursor()

        return _mock_conn()


@pytest.fixture
def mock_db_client() -> DatabricksClient:
    return MockDatabricksClient()


@pytest.fixture
def api_client(mock_db_client: DatabricksClient) -> Generator[TestClient, None, None]:
    """Test client with mocked Databricks dependency."""
    app.dependency_overrides[get_db_client] = lambda: mock_db_client
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture
def sample_telemetry_fixture() -> list[dict[str, Any]]:
    path = Path("tests/fixtures/sample_telemetry.json")
    if path.is_file():
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    return []


@pytest.fixture
def test_settings() -> Settings:
    return get_settings()

