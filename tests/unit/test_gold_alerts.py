"""Unit tests for Gold layer sliding-window alert evaluation engine."""

from datetime import datetime, timedelta, timezone
import pytest

from local_pipeline.alerts import evaluate_vehicle_alerts


def make_event(
    vehicle_id: str,
    dt: datetime,
    speed_kph: float = 50.0,
    engine_temp_c: float = 85.0,
    engine_status: int = 1,
    latitude: float = 37.7749,
    longitude: float = -122.4194,
) -> dict:
    """Helper creating a single clean telemetry event."""
    return {
        "vehicle_id": vehicle_id,
        "event_timestamp": dt.isoformat(),
        "speed_kph": speed_kph,
        "engine_temp_c": engine_temp_c,
        "engine_status": engine_status,
        "latitude": latitude,
        "longitude": longitude,
    }


def test_normal_cruising_vehicle_no_alerts():
    """Verify standard driving produces zero alerts and single state dict."""
    base_t = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)
    events = [
        make_event(
            "VH-1001",
            base_t + timedelta(seconds=i * 10),
            speed_kph=60.0,
            engine_temp_c=88.0 + (i * 0.2),
            latitude=37.7749 + (i * 0.001),
            longitude=-122.4194 + (i * 0.001),
        )
        for i in range(5)
    ]

    state, alerts = evaluate_vehicle_alerts("VH-1001", events)
    assert len(alerts) == 0
    assert state["vehicle_id"] == "VH-1001"
    assert state["current_speed_kph"] == 60.0
    assert state["engine_temp_c"] == 88.8
    assert state["engine_status"] == 1



def test_excessive_idle_alert():
    """Verify continuous engine on & speed 0 for > 10m triggers EXCESSIVE_IDLE."""
    base_t = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)

    # 1. Idling for 12 minutes (720s) -> Alert
    events_12m = [
        make_event("VH-1001", base_t + timedelta(minutes=i), speed_kph=0.0, engine_status=1)
        for i in range(13)
    ]
    _, alerts_12m = evaluate_vehicle_alerts("VH-1001", events_12m)
    alert_types = [a[0] for a in alerts_12m]
    assert "EXCESSIVE_IDLE" in alert_types

    # 2. Idling for 8 minutes (< 10m) -> No alert
    events_8m = [
        make_event("VH-1001", base_t + timedelta(minutes=i), speed_kph=0.0, engine_status=1)
        for i in range(9)
    ]
    _, alerts_8m = evaluate_vehicle_alerts("VH-1001", events_8m)
    assert "EXCESSIVE_IDLE" not in [a[0] for a in alerts_8m]

    # 3. Parked with engine off for 15m -> No idle alert
    events_parked = [
        make_event("VH-1001", base_t + timedelta(minutes=i), speed_kph=0.0, engine_status=0)
        for i in range(16)
    ]
    _, alerts_parked = evaluate_vehicle_alerts("VH-1001", events_parked)
    assert "EXCESSIVE_IDLE" not in [a[0] for a in alerts_parked]


def test_overheating_critical_alert():
    """Verify engine temp > 100°C for > 60s triggers OVERHEATING_CRITICAL."""
    base_t = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)

    # Hot for 80 seconds -> Alert
    events_hot = [
        make_event("VH-1002", base_t + timedelta(seconds=i * 20), engine_temp_c=105.0)
        for i in range(5)
    ]
    _, alerts_hot = evaluate_vehicle_alerts("VH-1002", events_hot)
    assert "OVERHEATING_CRITICAL" in [a[0] for a in alerts_hot]

    # Hot for only 30 seconds -> No alert
    events_short = [
        make_event("VH-1002", base_t + timedelta(seconds=i * 10), engine_temp_c=104.0)
        for i in range(4)
    ]
    _, alerts_short = evaluate_vehicle_alerts("VH-1002", events_short)
    assert "OVERHEATING_CRITICAL" not in [a[0] for a in alerts_short]


def test_harsh_braking_and_rapid_acceleration():
    """Verify consecutive differentials triggering HARSH_BRAKING and RAPID_ACCELERATION."""
    base_t = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)

    # Harsh brake: drops from 70 to 20 km/h in 2s (diff = -25 km/h/s < -15)
    events_brake = [
        make_event("VH-1003", base_t, speed_kph=70.0),
        make_event("VH-1003", base_t + timedelta(seconds=2), speed_kph=20.0),
    ]
    _, alerts_brake = evaluate_vehicle_alerts("VH-1003", events_brake)
    assert "HARSH_BRAKING" in [a[0] for a in alerts_brake]

    # Rapid accel: jumps from 20 to 60 km/h in 2s (diff = +20 km/h/s > +12)
    events_accel = [
        make_event("VH-1003", base_t, speed_kph=20.0),
        make_event("VH-1003", base_t + timedelta(seconds=2), speed_kph=60.0),
    ]
    _, alerts_accel = evaluate_vehicle_alerts("VH-1003", events_accel)
    assert "RAPID_ACCELERATION" in [a[0] for a in alerts_accel]


def test_overspeeding_alert():
    """Verify sustained speed > 110 km/h for >= 5m (300s) triggers OVERSPEEDING."""
    base_t = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)

    # Overspeeding for 360 seconds (6 mins) -> Alert
    events_fast = [
        make_event("VH-1004", base_t + timedelta(seconds=i * 60), speed_kph=125.0)
        for i in range(7)
    ]
    _, alerts_fast = evaluate_vehicle_alerts("VH-1004", events_fast)
    assert "OVERSPEEDING" in [a[0] for a in alerts_fast]

    # Overspeeding for only 180 seconds (3 mins) -> No alert
    events_brief = [
        make_event("VH-1004", base_t + timedelta(seconds=i * 60), speed_kph=125.0)
        for i in range(4)
    ]
    _, alerts_brief = evaluate_vehicle_alerts("VH-1004", events_brief)
    assert "OVERSPEEDING" not in [a[0] for a in alerts_brief]


def test_ghost_towing_anomaly():
    """Verify engine off with speed > 5 km/h or rapid GPS displacement triggers GHOST_TOWING."""
    base_t = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)

    # Case A: Engine off, speed sensor reports 25 km/h
    events_speed = [
        make_event("VH-1005", base_t, speed_kph=25.0, engine_status=0),
    ]
    _, alerts_speed = evaluate_vehicle_alerts("VH-1005", events_speed)
    assert "GHOST_TOWING" in [a[0] for a in alerts_speed]

    # Case B: Engine off, speed 0, but GPS coordinates translate rapidly (towed)
    events_gps_tow = [
        make_event("VH-1005", base_t, speed_kph=0.0, engine_status=0, latitude=37.7749, longitude=-122.4194),
        make_event("VH-1005", base_t + timedelta(seconds=10), speed_kph=0.0, engine_status=0, latitude=37.7790, longitude=-122.4194),
    ]
    _, alerts_gps = evaluate_vehicle_alerts("VH-1005", events_gps_tow)
    assert "GHOST_TOWING" in [a[0] for a in alerts_gps]


def test_cold_engine_hard_acceleration():
    """Verify high speed or hard accel with engine temp < 50°C triggers alert."""
    base_t = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)

    # High speed (75 km/h) while cold (38°C)
    events_cold = [
        make_event("VH-1006", base_t, speed_kph=75.0, engine_temp_c=38.0),
    ]
    _, alerts_cold = evaluate_vehicle_alerts("VH-1006", events_cold)
    assert "COLD_ENGINE_HARD_ACCEL" in [a[0] for a in alerts_cold]

    # High speed while warm (88°C) -> No alert
    events_warm = [
        make_event("VH-1006", base_t, speed_kph=75.0, engine_temp_c=88.0),
    ]
    _, alerts_warm = evaluate_vehicle_alerts("VH-1006", events_warm)
    assert "COLD_ENGINE_HARD_ACCEL" not in [a[0] for a in alerts_warm]


def test_stuck_sensor_anomaly():
    """Verify zero variance in GPS or temp across >= 5 moving events triggers STUCK_SENSOR_ANOMALY."""
    base_t = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)

    # Frozen GPS across 6 moving events
    events_frozen_gps = [
        make_event(
            "VH-1007",
            base_t + timedelta(seconds=i * 5),
            speed_kph=55.0,
            engine_temp_c=80.0 + i,
            latitude=37.7749,
            longitude=-122.4194,
        )
        for i in range(6)
    ]
    _, alerts_gps = evaluate_vehicle_alerts("VH-1007", events_frozen_gps)
    assert "STUCK_SENSOR_ANOMALY" in [a[0] for a in alerts_gps]

    # Frozen Temperature across 6 moving events
    events_frozen_temp = [
        make_event(
            "VH-1007",
            base_t + timedelta(seconds=i * 5),
            speed_kph=55.0,
            engine_temp_c=82.0,  # Zero variance
            latitude=37.7749 + (i * 0.001),
            longitude=-122.4194,
        )
        for i in range(6)
    ]
    _, alerts_temp = evaluate_vehicle_alerts("VH-1007", events_frozen_temp)
    assert "STUCK_SENSOR_ANOMALY" in [a[0] for a in alerts_temp]


def test_thermal_spike_at_idle():
    """Verify temp rise +5°C in <= 60s while stationary & idling triggers THERMAL_SPIKE_AT_IDLE."""
    base_t = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)

    # Temp spikes from 85°C to 92°C in 40s while stationary idling
    events_spike = [
        make_event("VH-1008", base_t, speed_kph=0.0, engine_temp_c=85.0, engine_status=1),
        make_event("VH-1008", base_t + timedelta(seconds=40), speed_kph=0.0, engine_temp_c=92.0, engine_status=1),
    ]
    _, alerts_spike = evaluate_vehicle_alerts("VH-1008", events_spike)
    assert "THERMAL_SPIKE_AT_IDLE" in [a[0] for a in alerts_spike]


def test_multiple_co_occurring_alerts():
    """Verify vehicle exhibiting multiple issues generates multiple active alerts."""
    base_t = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)

    # Overheating (> 100°C for > 60s) AND thermal spike at idle
    events = [
        make_event("VH-1009", base_t, speed_kph=0.0, engine_temp_c=98.0, engine_status=1),
        make_event("VH-1009", base_t + timedelta(seconds=30), speed_kph=0.0, engine_temp_c=104.0, engine_status=1),
        make_event("VH-1009", base_t + timedelta(seconds=95), speed_kph=0.0, engine_temp_c=106.0, engine_status=1),
    ]
    _, alerts = evaluate_vehicle_alerts("VH-1009", events)
    alert_names = [a[0] for a in alerts]
    assert "OVERHEATING_CRITICAL" in alert_names
    assert "THERMAL_SPIKE_AT_IDLE" in alert_names


def test_evaluate_vehicle_alerts_pandas_udf():
    """Verify Grouped Map Pandas UDF produces valid multi-row DataFrame."""
    import pandas as pd
    from local_pipeline.alerts import evaluate_vehicle_alerts_pandas

    base_t = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)
    # Vehicle with 2 alerts
    pdf = pd.DataFrame([
        make_event("VH-1010", base_t, speed_kph=0.0, engine_temp_c=98.0, engine_status=1),
        make_event("VH-1010", base_t + timedelta(seconds=30), speed_kph=0.0, engine_temp_c=104.0, engine_status=1),
        make_event("VH-1010", base_t + timedelta(seconds=95), speed_kph=0.0, engine_temp_c=106.0, engine_status=1),
    ])

    result_df = evaluate_vehicle_alerts_pandas(pdf)
    assert len(result_df) == 2
    assert set(result_df["alert_type"]) == {"OVERHEATING_CRITICAL", "THERMAL_SPIKE_AT_IDLE"}
    assert result_df["vehicle_id"].iloc[0] == "VH-1010"

    # Vehicle with normal state
    pdf_normal = pd.DataFrame([
        make_event("VH-1011", base_t, speed_kph=50.0, engine_temp_c=85.0, engine_status=1),
    ])
    result_normal = evaluate_vehicle_alerts_pandas(pdf_normal)
    assert len(result_normal) == 1
    assert result_normal["alert_type"].iloc[0] is None



