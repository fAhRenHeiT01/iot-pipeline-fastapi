"""Unit tests for the telemetry generator and anomaly injector."""

import json

from producer.generator import TelemetryGenerator
from producer.models import AnomalyType, TelemetryPayload


def test_generate_clean_event():
    gen = TelemetryGenerator(num_vehicles=10)
    event = gen.generate_clean_event("VH-1001")

    assert isinstance(event, TelemetryPayload)
    assert event.vehicle_id == "VH-1001"
    assert 0.0 <= event.speed_kph <= 110.0
    assert event.engine_status in (0, 1)


def test_generator_vehicle_pool():
    gen = TelemetryGenerator(num_vehicles=5)
    assert len(gen.vehicle_ids) == 5
    for _ in range(20):
        event = gen.generate_clean_event()
        assert event.vehicle_id in gen.vehicle_ids


def test_generator_terminal_garbage_injection():
    gen = TelemetryGenerator(
        num_vehicles=5,
        anomaly_rates={
            AnomalyType.TERMINAL_GARBAGE: 1.0,
            AnomalyType.NETWORK_DUPLICATE: 0.0,
            AnomalyType.LATE_ARRIVAL: 0.0,
            AnomalyType.SENSOR_NOISE: 0.0,
        },
    )

    payload_str, anomaly = gen.generate_event()
    assert anomaly == AnomalyType.TERMINAL_GARBAGE
    try:
        data = json.loads(payload_str)
        # Either null vehicle_id or bad timestamp
        assert data.get("vehicle_id") is None or data.get("event_timestamp") == "INVALID_TIMESTAMP_STRING"
    except json.JSONDecodeError:
        # Corrupt JSON mode
        assert "broken_json" in payload_str


def test_generator_sensor_noise_clamping_targets():
    gen = TelemetryGenerator(
        num_vehicles=5,
        anomaly_rates={
            AnomalyType.TERMINAL_GARBAGE: 0.0,
            AnomalyType.NETWORK_DUPLICATE: 0.0,
            AnomalyType.LATE_ARRIVAL: 0.0,
            AnomalyType.SENSOR_NOISE: 1.0,
        },
    )

    payload_str, anomaly = gen.generate_event()
    assert anomaly == AnomalyType.SENSOR_NOISE
    data = json.loads(payload_str)
    assert data["speed_kph"] < 0 or data["speed_kph"] > 160.0


def test_telemetry_payload_gps_flexibility():
    """Verify that TelemetryPayload allows null or out-of-bound coordinates for Bronze ingestion."""
    # Both null (connectivity issue)
    event_null = TelemetryPayload(latitude=None, longitude=None)
    assert event_null.latitude is None
    assert event_null.longitude is None

    # Out of bounds coordinates (sensor failure/garbage)
    event_corrupt = TelemetryPayload(latitude=125.0, longitude=-250.0)
    assert event_corrupt.latitude == 125.0
    assert event_corrupt.longitude == -250.0

