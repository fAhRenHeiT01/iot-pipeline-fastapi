"""End-to-end API verification and SLA freshness checks."""

import pytest


@pytest.mark.integration
def test_api_e2e_freshness_flow(api_client):
    """Verify complete API flow from root -> health -> fleet status -> metrics."""
    # 1. Root check
    root_res = api_client.get("/")
    assert root_res.status_code == 200

    # 2. Health check (SLA compliance)
    health_res = api_client.get("/health")
    assert health_res.status_code == 200
    health_data = health_res.json()
    assert health_data["database_connected"] is True
    assert health_data["sla_compliant"] is True

    # 3. Fleet status query
    fleet_res = api_client.get("/fleet/status?limit=10")
    assert fleet_res.status_code == 200
    fleet_data = fleet_res.json()
    assert fleet_data["total_count"] > 0

    # 4. Prometheus metrics check
    metrics_res = api_client.get("/metrics")
    assert metrics_res.status_code == 200
    assert "fleet_database_connected" in metrics_res.text
    assert "fleet_active_vehicles_total" in metrics_res.text

