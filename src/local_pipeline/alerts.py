"""Alert Evaluation Engine for Local Gold Lakehouse Layer.

Evaluates 8 critical fleet telemetry alerts against a 10-minute sliding event window:
1. EXCESSIVE_IDLE: Engine on & speed 0 continuously exceeding 10 minutes.
2. OVERHEATING_CRITICAL: Engine temperature > 100°C continuously for > 60 seconds.
3. HARSH_BRAKING: Consecutive event differential Δspeed / Δt < -15 km/h/s.
4. RAPID_ACCELERATION: Consecutive event differential Δspeed / Δt > +12 km/h/s.
5. OVERSPEEDING: Sustained speed > 110 km/h for >= 5 minutes.
6. GHOST_TOWING: Engine off with speed > 5 km/h or rapid GPS coordinate translation.
7. COLD_ENGINE_HARD_ACCEL: High speed (> 60 km/h) or rapid acceleration while engine temp < 50°C.
8. STUCK_SENSOR_ANOMALY: Zero variance in GPS coordinates or temperature across >= 5 moving events.
9. THERMAL_SPIKE_AT_IDLE: Rapid temperature rise (+5°C in <= 60s or >= 0.1°C/s) while stationary and idling.
"""

from datetime import datetime, timezone
import math
from typing import Any


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate great-circle distance between two GPS coordinates in kilometers."""
    radius_earth_km = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (
        math.sin(dphi / 2.0) ** 2
        + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2.0) ** 2
    )
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return radius_earth_km * c


def parse_timestamp(ts: Any) -> datetime:
    """Normalize string, int epoch, or datetime to UTC timezone-aware datetime."""
    if isinstance(ts, datetime):
        return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    clean = str(ts).replace("Z", "+00:00")
    dt = datetime.fromisoformat(clean)
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def evaluate_vehicle_alerts(
    vehicle_id: str,
    events: list[dict[str, Any]],
) -> tuple[dict[str, Any], list[tuple[str, str]]]:
    """Evaluate sliding window telemetry events for a vehicle and detect active alerts.

    Args:
        vehicle_id: Target vehicle identifier.
        events: Chronologically sorted list of clean Silver events for the vehicle
                within the 10-minute sliding window.

    Returns:
        tuple of:
          - latest_state: Dict containing current vehicle status metrics.
          - active_alerts: List of (alert_type, alert_details) tuples.
    """
    if not events:
        raise ValueError(f"Cannot evaluate alerts for vehicle {vehicle_id} with empty events list")

    # Sort chronologically by event_timestamp
    parsed_events = []
    for ev in events:
        ev_copy = dict(ev)
        ev_copy["dt"] = parse_timestamp(ev["event_timestamp"])
        parsed_events.append(ev_copy)
    parsed_events.sort(key=lambda x: x["dt"])

    latest = parsed_events[-1]
    latest_dt = latest["dt"]
    latest_speed = float(latest.get("speed_kph", 0.0))
    latest_temp = (
        float(latest["engine_temp_c"]) if latest.get("engine_temp_c") is not None else None
    )
    latest_engine_status = (
        int(latest["engine_status"]) if latest.get("engine_status") is not None else None
    )
    latest_lat = float(latest["latitude"]) if latest.get("latitude") is not None else None
    latest_lon = float(latest["longitude"]) if latest.get("longitude") is not None else None

    latest_state = {
        "vehicle_id": vehicle_id,
        "last_event_timestamp": latest_dt.isoformat(),
        "latitude": latest_lat,
        "longitude": latest_lon,
        "current_speed_kph": latest_speed,
        "engine_temp_c": latest_temp,
        "engine_status": latest_engine_status,
    }

    active_alerts: list[tuple[str, str]] = []

    # -------------------------------------------------------------------------
    # 1. EXCESSIVE_IDLE Alert: engine_status == 1 & speed == 0 for > 10 mins (600s)
    # -------------------------------------------------------------------------
    if latest_engine_status == 1 and latest_speed == 0.0:
        idle_start_dt = latest_dt
        for ev in reversed(parsed_events):
            if ev.get("engine_status") == 1 and float(ev.get("speed_kph", 0.0)) == 0.0:
                idle_start_dt = ev["dt"]
            else:
                break
        idle_seconds = (latest_dt - idle_start_dt).total_seconds()
        if idle_seconds > 600.0:
            active_alerts.append((
                "EXCESSIVE_IDLE",
                f"Vehicle idling continuously for {idle_seconds / 60.0:.1f} minutes (> 10m threshold)",
            ))

    # -------------------------------------------------------------------------
    # 2. OVERHEATING_CRITICAL Alert: engine_temp > 100°C for > 60 seconds
    # -------------------------------------------------------------------------
    if latest_temp is not None and latest_temp >= 75.0:
        overheat_start_dt = latest_dt
        for ev in reversed(parsed_events):
            t = ev.get("engine_temp_c")
            if t is not None and float(t) >= 75.0:
                overheat_start_dt = ev["dt"]
            else:
                break
        overheat_seconds = (latest_dt - overheat_start_dt).total_seconds()
        if overheat_seconds > 60.0:
            active_alerts.append((
                "OVERHEATING_CRITICAL",
                f"Engine temperature {latest_temp:.1f}°C exceeded 100°C continuously for {overheat_seconds:.0f}s (> 60s threshold)",
            ))

    # -------------------------------------------------------------------------
    # 3. HARSH_BRAKING (< -15 km/h/s) & RAPID_ACCELERATION (> +12 km/h/s)
    # -------------------------------------------------------------------------
    recent_accel = 0.0
    if len(parsed_events) >= 2:
        prev = parsed_events[-2]
        delta_t = (latest_dt - prev["dt"]).total_seconds()
        if 0.0 < delta_t <= 15.0:
            recent_accel = (latest_speed - float(prev.get("speed_kph", 0.0))) / delta_t
            if recent_accel < -15.0:
                active_alerts.append((
                    "HARSH_BRAKING",
                    f"Harsh braking detected: deceleration {recent_accel:.1f} km/h/s (< -15 km/h/s)",
                ))
            elif recent_accel > 12.0:
                active_alerts.append((
                    "RAPID_ACCELERATION",
                    f"Rapid acceleration detected: {recent_accel:.1f} km/h/s (> +12 km/h/s)",
                ))

    # -------------------------------------------------------------------------
    # 4. OVERSPEEDING Alert: Sustained speed > 110 km/h for >= 5 mins (300s)
    # -------------------------------------------------------------------------
    if latest_speed > 110.0:
        speed_start_dt = latest_dt
        for ev in reversed(parsed_events):
            if float(ev.get("speed_kph", 0.0)) > 110.0:
                speed_start_dt = ev["dt"]
            else:
                break
        overspeed_seconds = (latest_dt - speed_start_dt).total_seconds()
        if overspeed_seconds >= 300.0:
            active_alerts.append((
                "OVERSPEEDING",
                f"Sustained overspeeding at {latest_speed:.1f} km/h for {overspeed_seconds / 60.0:.1f} minutes (>= 5m threshold)",
            ))

    # -------------------------------------------------------------------------
    # 5. GHOST_TOWING / Rollaway Anomaly: engine_status == 0 & (speed > 5 km/h or GPS transit)
    # -------------------------------------------------------------------------
    if latest_engine_status == 0:
        if latest_speed > 5.0:
            active_alerts.append((
                "GHOST_TOWING",
                f"Ghost towing/rollaway: vehicle moving at {latest_speed:.1f} km/h with engine off",
            ))
        elif len(parsed_events) >= 2:
            prev = parsed_events[-2]
            delta_t = (latest_dt - prev["dt"]).total_seconds()
            prev_lat = prev.get("latitude")
            prev_lon = prev.get("longitude")
            if (
                delta_t > 0.0
                and latest_lat is not None
                and latest_lon is not None
                and prev_lat is not None
                and prev_lon is not None
            ):
                dist_km = haversine_km(
                    float(prev_lat), float(prev_lon), latest_lat, latest_lon
                )
                gps_speed_kph = (dist_km / delta_t) * 3600.0
                if gps_speed_kph > 5.0:
                    active_alerts.append((
                        "GHOST_TOWING",
                        f"Ghost towing/rollaway: rapid GPS displacement {gps_speed_kph:.1f} km/h with engine off",
                    ))

    # -------------------------------------------------------------------------
    # 6. COLD_ENGINE_HARD_ACCEL: Engine temp < 50°C while high speed (> 60 km/h) or rapid accel (> 12)
    # -------------------------------------------------------------------------
    if latest_temp is not None and latest_temp < 50.0:
        if latest_speed > 60.0 or recent_accel > 12.0:
            active_alerts.append((
                "COLD_ENGINE_HARD_ACCEL",
                f"Hard vehicle stress on cold engine: {latest_speed:.1f} km/h (temp {latest_temp:.1f}°C < 50°C)",
            ))

    # -------------------------------------------------------------------------
    # 7. STUCK_SENSOR_ANOMALY: Zero variance in coords or temp across >= 5 moving events
    # -------------------------------------------------------------------------
    moving_events = [ev for ev in parsed_events if float(ev.get("speed_kph", 0.0)) > 10.0]
    if len(moving_events) >= 5:
        # Check GPS coordinates variance while moving
        coords = [
            (float(ev["latitude"]), float(ev["longitude"]))
            for ev in moving_events
            if ev.get("latitude") is not None and ev.get("longitude") is not None
        ]
        if len(coords) >= 5:
            lats = [c[0] for c in coords]
            lons = [c[1] for c in coords]
            if max(lats) == min(lats) and max(lons) == min(lons):
                active_alerts.append((
                    "STUCK_SENSOR_ANOMALY",
                    f"Stuck GPS coordinates ({lats[0]:.4f}, {lons[0]:.4f}) across {len(coords)} moving events",
                ))

        # Check temperature variance while moving
        temps = [
            float(ev["engine_temp_c"])
            for ev in moving_events
            if ev.get("engine_temp_c") is not None
        ]
        if len(temps) >= 5 and max(temps) == min(temps):
            active_alerts.append((
                "STUCK_SENSOR_ANOMALY",
                f"Stuck temperature reading ({temps[0]:.1f}°C) with zero variance across {len(temps)} moving events",
            ))

    # -------------------------------------------------------------------------
    # 8. THERMAL_SPIKE_AT_IDLE: Engine temp rising rapidly (+5°C in <= 60s or >= 0.1°C/s) while stationary
    # -------------------------------------------------------------------------
    if latest_engine_status == 1 and latest_speed == 0.0 and latest_temp is not None:
        # Collect continuous stationary idle events leading up to latest
        idle_events = []
        for ev in reversed(parsed_events):
            if (
                ev.get("engine_status") == 1
                and float(ev.get("speed_kph", 0.0)) == 0.0
                and ev.get("engine_temp_c") is not None
            ):
                idle_events.append(ev)
            else:
                break
        idle_events.reverse()

        if len(idle_events) >= 2:
            spike_detected = False
            spike_details = ""
            for i in range(len(idle_events)):
                for j in range(i + 1, len(idle_events)):
                    e_start = idle_events[i]
                    e_end = idle_events[j]
                    delta_sec = (e_end["dt"] - e_start["dt"]).total_seconds()
                    if 10.0 <= delta_sec <= 120.0:
                        delta_temp = float(e_end["engine_temp_c"]) - float(e_start["engine_temp_c"])
                        rate = delta_temp / delta_sec
                        if (delta_sec <= 60.0 and delta_temp >= 5.0) or (delta_sec >= 20.0 and rate >= 0.08):
                            spike_detected = True
                            spike_details = (
                                f"Thermal spike while idling: temp rose +{delta_temp:.1f}°C in "
                                f"{delta_sec:.0f}s (rate {rate:.2f}°C/s)"
                            )
                            break
                if spike_detected:
                    break

            if spike_detected:
                active_alerts.append(("THERMAL_SPIKE_AT_IDLE", spike_details))


    return latest_state, active_alerts


def evaluate_vehicle_alerts_pandas(pdf: Any) -> Any:
    """Grouped Map Pandas UDF for PySpark Structured Streaming.

    Runs distributed across Spark executor cores with Apache Arrow zero-copy serialization,
    bypassing the Python GIL via multi-process worker pools.

    Args:
        pdf: Pandas DataFrame containing events for a single vehicle_id.

    Returns:
        Pandas DataFrame conforming to GOLD_STATUS_SCHEMA.
    """
    import pandas as pd

    cols = [
        "vehicle_id",
        "last_event_timestamp",
        "latitude",
        "longitude",
        "current_speed_kph",
        "engine_temp_c",
        "engine_status",
        "alert_type",
        "alert_details",
        "last_updated_at",
    ]
    if pdf.empty:
        return pd.DataFrame(columns=cols)

    vehicle_id = str(pdf["vehicle_id"].iloc[0])
    events = pdf.to_dict(orient="records")

    latest_state, active_alerts = evaluate_vehicle_alerts(vehicle_id, events)

    now_ts = pd.Timestamp.now(tz="UTC")
    rows = []
    if active_alerts:
        for alert_type, alert_details in active_alerts:
            rows.append({
                "vehicle_id": vehicle_id,
                "last_event_timestamp": pd.to_datetime(latest_state["last_event_timestamp"]),
                "latitude": float(latest_state["latitude"]) if latest_state["latitude"] is not None else None,
                "longitude": float(latest_state["longitude"]) if latest_state["longitude"] is not None else None,
                "current_speed_kph": float(latest_state["current_speed_kph"]),
                "engine_temp_c": float(latest_state["engine_temp_c"]) if latest_state["engine_temp_c"] is not None else None,
                "engine_status": int(latest_state["engine_status"]) if latest_state["engine_status"] is not None else None,
                "alert_type": alert_type,
                "alert_details": alert_details,
                "last_updated_at": now_ts,
            })
    else:
        rows.append({
            "vehicle_id": vehicle_id,
            "last_event_timestamp": pd.to_datetime(latest_state["last_event_timestamp"]),
            "latitude": float(latest_state["latitude"]) if latest_state["latitude"] is not None else None,
            "longitude": float(latest_state["longitude"]) if latest_state["longitude"] is not None else None,
            "current_speed_kph": float(latest_state["current_speed_kph"]),
            "engine_temp_c": float(latest_state["engine_temp_c"]) if latest_state["engine_temp_c"] is not None else None,
            "engine_status": int(latest_state["engine_status"]) if latest_state["engine_status"] is not None else None,
            "alert_type": None,
            "alert_details": None,
            "last_updated_at": now_ts,
        })

    return pd.DataFrame(rows)

