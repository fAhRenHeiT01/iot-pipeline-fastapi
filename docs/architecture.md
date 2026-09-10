# Architecture & Technical Trade-offs

## 1. Overview

The Fleet Real-time Lakehouse pipeline processes high-frequency IoT telemetry from connected vehicles, guarantees end-to-end SLA freshness within 120 seconds, ensures resilient data quality isolation via a Dead Letter Queue (DLQ), and serves operational intelligence through FastAPI.

```
[IoT Generator CLI]
        │
        ▼ (SASL_SSL / JSON payloads)
[Confluent Cloud Kafka: fleet.telemetry.raw]
        │
        ▼
[Bronze: bronze_fleet_raw (Append-only Delta)]
        │
        ├───────────────────────────────────────────────────────┐
        ▼ (Split & Filter)                                      ▼
[Silver DLQ: silver_fleet_quarantine]           [Silver: silver_fleet_events (Clean Delta)]
(Malformed JSON, null vehicle_id, bad ts)       (15-min Watermark, Dedup on [vid, ts], Clamped speed)
                                                                │
                                                                ▼ (Micro-batch MERGE INTO)
                                                [Gold: gold_vehicle_status (Delta Table)]
                                                (Current state, idle duration, alert flags)
                                                                │
                                                                ▼
                                                     [FastAPI Serving Layer]
                                             ├── GET /fleet/status (Status/Alerts)
                                             ├── GET /health (SLA Lag < 120s)
                                             └── GET /metrics (Prometheus stats)
```

## 2. Lakehouse Design & Storage Strategy

### Bronze (Raw Append-only)
- **Table**: `fleet_iot.telemetry.bronze_fleet_raw`
- **Characteristics**: Immutable, append-only log capturing raw Kafka payloads along with Kafka message metadata (topic, partition, offset, timestamp).
- **Justification**: Protects against upstream data loss and enables zero-loss reprocessing in the event of pipeline schema adjustments.

### Silver (Clean & Quarantine DLQ)
- **Tables**:
  - `fleet_iot.telemetry.silver_fleet_events`: Cleaned, typed events partitioned by `DATE(event_timestamp)`.
  - `fleet_iot.telemetry.silver_fleet_quarantine`: Dead Letter Queue for terminal garbage.
- **Handling Rules**:
  - **Terminal Garbage**: Records with malformed JSON, `NULL` vehicle ID, or unparseable timestamps are immediately diverted to quarantine without blocking the clean stream.
  - **Network Duplicates**: Deduplicated within the 15-minute watermark window on `(vehicle_id, event_timestamp)`.
  - **Sensor Drift**: Speeds `< 0` or `> 160 kph` are clamped to valid boundaries, and flagged (`speed_clamped = true`) while retaining `raw_speed_kph` for diagnostic audits.

### Gold (Operational Status)
- **Table**: `fleet_iot.telemetry.gold_vehicle_status`
- **Characteristics**: Upserted via micro-batch `MERGE INTO` keyed on `vehicle_id`.
- **Derived Metrics**: Evaluates `is_idle`, calculates duration in minutes, and sets alerting flags (`has_speed_alert`).

## 3. Serving & Freshness SLA

- **SLA Target**: Telemetry event latency from vehicle generation to Gold table availability must remain `< 120 seconds`.
- **Health Endpoint**: `GET /health` continuously measures `NOW() - MAX(last_event_timestamp)` to ensure strict adherence.
- **Databricks SQL Serverless**: Serves high-concurrency low-latency queries directly into the FastAPI layer.

