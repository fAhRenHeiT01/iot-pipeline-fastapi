"""Unit tests verifying FastAPI endpoints and response contracts."""


def test_root_endpoint(api_client):
    response = api_client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "online"


def test_fleet_status_endpoint(api_client):
    response = api_client.get("/fleet/status")
    assert response.status_code == 200
    data = response.json()
    assert "total_count" in data
    assert "vehicles" in data
    assert len(data["vehicles"]) >= 1
    first_vehicle = data["vehicles"][0]
    assert "vehicle_id" in first_vehicle
    assert "current_speed_kph" in first_vehicle


def test_health_check_endpoint(api_client):
    response = api_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["database_connected"] is True
    assert "status" in data
    assert "sla_compliant" in data


def test_health_check_endpoint_offline_unconnected():
    from contextlib import contextmanager
    from unittest.mock import MagicMock

    from fastapi.testclient import TestClient

    from api.dependencies import DatabricksClient, get_db_client
    from api.main import app
    from common.config import Settings

    mock_client = MagicMock(spec=DatabricksClient)
    mock_client.settings = Settings(ENVIRONMENT="test")

    @contextmanager
    def _mock_conn():
        yield None

    mock_client.get_connection = _mock_conn

    app.dependency_overrides[get_db_client] = lambda: mock_client
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["database_connected"] is False
        assert data["latest_event_latency_seconds"] is None
        assert data["sla_compliant"] is False
        assert data["status"] == "unhealthy"
    app.dependency_overrides.clear()


def test_health_check_endpoint_degraded_latency():
    from contextlib import contextmanager
    from datetime import datetime, timedelta
    from unittest.mock import MagicMock

    from fastapi.testclient import TestClient

    from api.dependencies import DatabricksClient, get_db_client
    from api.main import app
    from common.config import Settings

    mock_client = MagicMock(spec=DatabricksClient)
    mock_client.settings = Settings(ENVIRONMENT="test", SLA_MAX_LATENCY_SECONDS=120)

    old_ts = datetime.now() - timedelta(seconds=300)
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (old_ts,)

    @contextmanager
    def _mock_conn():
        yield mock_cursor

    mock_client.get_connection = _mock_conn

    app.dependency_overrides[get_db_client] = lambda: mock_client
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["database_connected"] is True
        assert data["sla_compliant"] is False
        assert data["status"] == "degraded"
        assert data["latest_event_latency_seconds"] >= 299.0
    app.dependency_overrides.clear()


def test_health_check_endpoint_none_timestamp():
    from contextlib import contextmanager
    from unittest.mock import MagicMock

    from fastapi.testclient import TestClient

    from api.dependencies import DatabricksClient, get_db_client
    from api.main import app
    from common.config import Settings

    mock_client = MagicMock(spec=DatabricksClient)
    mock_client.settings = Settings(ENVIRONMENT="test")

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (None,)

    @contextmanager
    def _mock_conn():
        yield mock_cursor

    mock_client.get_connection = _mock_conn

    app.dependency_overrides[get_db_client] = lambda: mock_client
    with TestClient(app) as client:
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["database_connected"] is True
        assert data["latest_event_latency_seconds"] is None
        assert data["sla_compliant"] is False
        assert data["status"] == "degraded"
    app.dependency_overrides.clear()



def test_metrics_endpoint(api_client):
    # Perform a request first to ensure the middleware records it
    api_client.get("/")
    response = api_client.get("/metrics")
    assert response.status_code == 200
    text = response.text
    assert "fleet_api_requests_total" in text
    assert 'endpoint="/"' in text
    assert "fleet_database_connected" in text
    assert "fleet_active_vehicles_total" in text
    assert "fleet_telemetry_lag_seconds" in text
    assert "fleet_telemetry_sla_compliant" in text

