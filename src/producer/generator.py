"""Telemetry generator producing clean and dirty events for pipeline testing."""

from datetime import datetime, timedelta, timezone
import json
import random
from typing import Any

from producer.models import AnomalyType, TelemetryPayload


class TelemetryGenerator:
    """Generates synthetic fleet telemetry with configurable anomaly injection."""

    def __init__(
        self,
        num_vehicles: int = 50,
        anomaly_rates: dict[AnomalyType, float] | None = None,
    ):
        self.num_vehicles = num_vehicles
        self.vehicle_ids = [f"VH-{1000 + i}" for i in range(num_vehicles)]
        self.anomaly_rates = anomaly_rates or {
            AnomalyType.TERMINAL_GARBAGE: 0.03,
            AnomalyType.NETWORK_DUPLICATE: 0.05,
            AnomalyType.LATE_ARRIVAL: 0.04,
            AnomalyType.SENSOR_NOISE: 0.05,
        }
        self._last_emitted_event: dict[str, Any] | None = None

    def generate_clean_event(self, vehicle_id: str | None = None) -> TelemetryPayload:
        """Generate a valid, in-bounds telemetry event."""
        vid = vehicle_id or random.choice(self.vehicle_ids)
        engine_status = 0 if random.random() < 0.10 else 1
        speed = 0.0 if engine_status == 0 or random.random() < 0.15 else round(random.uniform(5.0, 110.0), 2)

        return TelemetryPayload(
            vehicle_id=vid,
            event_timestamp=int(datetime.now(timezone.utc).timestamp()),
            latitude=round(random.uniform(37.60, 37.85), 6),
            longitude=round(random.uniform(-122.50, -122.35), 6),
            speed_kph=speed,
            engine_temp_c=round(random.uniform(35.0, 105.0), 1),
            engine_status=engine_status,
            odometer_km=round(random.uniform(10000.0, 85000.0), 1),
        )

    def generate_event(self) -> tuple[str, AnomalyType]:
        """Generate an event as a serialized JSON string along with its classification."""
        roll = random.random()
        cumulative = 0.0

        # 1. Terminal Garbage
        cumulative += self.anomaly_rates.get(AnomalyType.TERMINAL_GARBAGE, 0.0)
        if roll < cumulative:
            garbage_mode = random.choice(["corrupt_json", "null_vehicle_id", "bad_timestamp"])
            if garbage_mode == "corrupt_json":
                return '{"vehicle_id": "VH-9999", "speed_kph": broken_json', AnomalyType.TERMINAL_GARBAGE
            if garbage_mode == "null_vehicle_id":
                event = self.generate_clean_event()
                data = event.model_dump()
                data["vehicle_id"] = None
                return json.dumps(data), AnomalyType.TERMINAL_GARBAGE
            # bad_timestamp
            event = self.generate_clean_event()
            data = event.model_dump()
            data["event_timestamp"] = "INVALID_TIMESTAMP_STRING"
            return json.dumps(data), AnomalyType.TERMINAL_GARBAGE

        # 2. Network Duplicate
        cumulative += self.anomaly_rates.get(AnomalyType.NETWORK_DUPLICATE, 0.0)
        if roll < cumulative and self._last_emitted_event:
            return json.dumps(self._last_emitted_event), AnomalyType.NETWORK_DUPLICATE

        # 3. Late Arrival (>15 min delay)
        cumulative += self.anomaly_rates.get(AnomalyType.LATE_ARRIVAL, 0.0)
        if roll < cumulative:
            event = self.generate_clean_event()
            data = event.model_dump()
            lag_minutes = random.uniform(16.0, 45.0)
            delayed_time = datetime.now(timezone.utc) - timedelta(minutes=lag_minutes)
            data["event_timestamp"] = int(delayed_time.timestamp())
            self._last_emitted_event = data
            return json.dumps(data), AnomalyType.LATE_ARRIVAL

        # 4. Sensor Noise / Drift
        cumulative += self.anomaly_rates.get(AnomalyType.SENSOR_NOISE, 0.0)
        if roll < cumulative:
            event = self.generate_clean_event()
            data = event.model_dump()
            noise_type = random.choice(["negative_speed", "excessive_speed"])
            if noise_type == "negative_speed":
                data["speed_kph"] = round(random.uniform(-50.0, -1.0), 2)
            else:
                data["speed_kph"] = round(random.uniform(165.0, 240.0), 2)
            self._last_emitted_event = data
            return json.dumps(data), AnomalyType.SENSOR_NOISE

        # Clean event
        clean_event = self.generate_clean_event()
        event_dict = clean_event.model_dump()
        self._last_emitted_event = event_dict
        return json.dumps(event_dict), AnomalyType.CLEAN

