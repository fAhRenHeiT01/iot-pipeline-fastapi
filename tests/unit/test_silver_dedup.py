"""Unit tests for Silver deduplication and watermark logic."""

from datetime import datetime, timedelta, timezone


def is_within_watermark(
    event_timestamp: datetime, current_watermark: datetime
) -> bool:
    """Check if event timestamp is after the 10-minute watermark boundary."""
    return event_timestamp >= current_watermark


def test_watermark_boundary_filtering():
    base_time = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)
    watermark = base_time - timedelta(minutes=10)

    on_time_event = base_time - timedelta(minutes=5)
    boundary_event = base_time - timedelta(minutes=10)
    late_event = base_time - timedelta(minutes=15)

    assert is_within_watermark(on_time_event, watermark) is True
    assert is_within_watermark(boundary_event, watermark) is True
    assert is_within_watermark(late_event, watermark) is False



def test_deduplicate_events():
    events = [
        {"vehicle_id": "VH-1001", "event_timestamp": "2026-09-08T12:00:00Z", "seq": 1},
        {"vehicle_id": "VH-1001", "event_timestamp": "2026-09-08T12:00:00Z", "seq": 2},  # dup
        {"vehicle_id": "VH-1001", "event_timestamp": "2026-09-08T12:01:00Z", "seq": 3},
        {"vehicle_id": "VH-1002", "event_timestamp": "2026-09-08T12:00:00Z", "seq": 4},
    ]

    seen = set()
    deduped = []
    for ev in events:
        key = (ev["vehicle_id"], ev["event_timestamp"])
        if key not in seen:
            seen.add(key)
            deduped.append(ev)

    assert len(deduped) == 3
    assert [e["seq"] for e in deduped] == [1, 3, 4]

