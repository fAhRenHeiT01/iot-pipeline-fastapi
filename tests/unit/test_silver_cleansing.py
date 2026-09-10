"""Unit tests for Silver layer data cleansing and speed clamping logic."""

import pytest


def clamp_speed(raw_speed: float) -> tuple[float, bool]:
    """Helper representing the Silver layer clamping expression:

    when(speed < 0, 0).when(speed > 160, 160).otherwise(speed)
    """
    if raw_speed < 0.0:
        return 0.0, True
    if raw_speed > 160.0:
        return 160.0, True
    return raw_speed, False


def is_terminal_garbage(
    vehicle_id: str | None,
    timestamp_valid: bool,
    latitude: float | None = None,
    longitude: float | None = None,
) -> bool:
    """Helper representing the Silver quarantine condition including GPS validation."""
    if vehicle_id is None or not timestamp_valid:
        return True
    # GPS validation: mismatched coordinates (one null, other not)
    if (latitude is None and longitude is not None) or (latitude is not None and longitude is None):
        return True
    # GPS validation: out of bounds coordinates
    if latitude is not None and (latitude < -90.0 or latitude > 90.0):
        return True
    if longitude is not None and (longitude < -180.0 or longitude > 180.0):
        return True
    return False






@pytest.mark.parametrize(
    "raw_speed,expected_clamped,expected_flag",
    [
        (55.0, 55.0, False),
        (0.0, 0.0, False),
        (160.0, 160.0, False),
        (-15.5, 0.0, True),
        (210.0, 160.0, True),
    ],
)
def test_speed_clamping_logic(raw_speed: float, expected_clamped: float, expected_flag: bool):
    clamped, flag = clamp_speed(raw_speed)
    assert clamped == expected_clamped
    assert flag == expected_flag


@pytest.mark.parametrize(
    "vehicle_id,timestamp_valid,latitude,longitude,expected_quarantine",
    [
        ("VH-1001", True, 37.7749, -122.4194, False),
        ("VH-1001", True, None, None, False),  # Paired null GPS is valid (sensor issue, clean event)
        (None, True, 37.7749, -122.4194, True),
        ("VH-1002", False, 37.7749, -122.4194, True),
        (None, False, 37.7749, -122.4194, True),
        ("VH-1001", True, 37.7749, None, True),  # Mismatched lat without lon
        ("VH-1001", True, None, -122.4194, True),  # Mismatched lon without lat
        ("VH-1001", True, 95.0, -122.4194, True),  # Out of bounds lat
        ("VH-1001", True, 37.7749, -190.0, True),  # Out of bounds lon
    ],
)
def test_quarantine_identification(
    vehicle_id: str | None,
    timestamp_valid: bool,
    latitude: float | None,
    longitude: float | None,
    expected_quarantine: bool,
):
    assert (
        is_terminal_garbage(vehicle_id, timestamp_valid, latitude, longitude)
        == expected_quarantine
    )





