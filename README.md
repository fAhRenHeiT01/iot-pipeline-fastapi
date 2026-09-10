# Fleet Real-time Lakehouse & IoT Pipeline

High-throughput, resilient IoT Fleet Telemetry Lakehouse built on Databricks Structured Streaming, Delta Lake, FastAPI Serving Layer, and a local PySpark & SQLite streaming pipeline.

---

## Architecture Overview

```
[IoT Telemetry CLI Generator]
        │ (SASL/SSL Plaintext / JSON payloads)
        ▼
[Confluent Cloud Kafka: fleet.telemetry.raw]
        │
        ▼
[Bronze: bronze_fleet_raw (Append-only Parquet / SQLite)]
        │ (WAL Offset Checkpointing & Idempotent (topic, partition, offset) uniqueness)
        │
        ├───────────────────────────────────────────────────────┐
        ▼ (Terminal Garbage Separation)                         ▼
[Silver DLQ: silver_fleet_quarantine]           [Silver: silver_fleet_events (Clean Parquet / SQLite)]
(Malformed JSON, null vehicle_id, bad ts)       (10-min Watermark, Dedup on [vid, ts], Clamped speed,
                                                 Physical layout sorting: sortWithinPartitions)
                                                                │
                                                                ▼ (10-min Sliding Window & Alert Engine)
                                                [Gold: gold_vehicle_status (Multi-row Alerts / SQLite)]
                                                (Multi-row alerts, selective atomic refresh per vehicle,
                                                 8 telemetry & safety alert rules evaluated)
                                                                │
                                                                ▼
                                                     [FastAPI Serving Layer]
                                             ├── GET /fleet/status (Status/Alerts)
                                             ├── GET /health (SLA Lag < 120s)
                                             └── GET /metrics (Prometheus stats)
```

---

## Medallion Architecture Processing (`local_pipeline`)

The `local_pipeline` reproduces the complete lakehouse Medallion Architecture locally using PySpark Structured Streaming, local Parquet storage, and SQLite:

### 1. Bronze Layer Processing
- **Dual Ingestion Sources**: Ingests either live streaming telemetry from Confluent Cloud Kafka (`--source kafka`) or simulated multi-vehicle streaming data from an offline rate generator (`--source generator`).
- **Write-Ahead Log (WAL) Checkpointing**: PySpark Structured Streaming tracks Kafka offsets into write-ahead logs (`data/checkpoints/bronze`), guaranteeing exactly-once micro-batch offset advancement.
- **Idempotent Storage**: Micro-batches are persisted to SQLite `bronze_fleet_raw` with a composite `UNIQUE(kafka_topic, kafka_partition, kafka_offset)` constraint using `INSERT OR IGNORE`, preventing duplicate rows across network retries.
- **Parquet Storage**: Appends raw micro-batches to local lakehouse Parquet storage (`data/lakehouse/bronze`) with automated directory retention pruning.

### 2. Silver Layer Processing
- **Dead Letter Queue (DLQ) Quarantine**: Validates incoming raw JSON payloads and immediately quarantines terminal garbage (corrupt JSON, missing `vehicle_id`, unparseable timestamps, or corrupted/mismatched GPS coordinates) into `silver_fleet_quarantine`.
- **Event-Time Watermarking (10 Minutes)**: Evaluates event timestamps against a 10-minute watermark boundary (`max_event_ts - 10 minutes`), dropping unrecoverable late-arriving packets and bounding streaming state-store lifecycles.
- **Stateful Deduplication**: Drops device bursts and network re-transmissions by deduplicating records on the composite key `(vehicle_id, event_timestamp)` within the watermark window.
- **Sensor Clamping**: Normalizes vehicle speed into valid physical bounds $[0.0, 160.0]\text{ km/h}$ and sets a boolean flag `speed_clamped`. Idle state deduction is removed from Silver and computed dynamically in Gold.
- **Physical Layout Optimization**: Sorts records via `.sortWithinPartitions("vehicle_id", "event_timestamp")` before writing to Parquet (`data/lakehouse/silver`), ensuring row-group min/max statistics maximize data-skipping efficiency. Indexed in SQLite via `idx_silver_events_vid_ts`.

### 3. Gold Layer Processing
- **10-Minute Sliding Window Horizon**: For each active vehicle in the micro-batch, queries its 10-minute event timeline from the indexed Silver store ending at the batch's latest event timestamp.
- **Multi-Row Alert Schema**: Replaces legacy flags (`is_idle`, `idle_duration`, `has_speed_alert`) with explicit `alert_type` and `alert_details` columns. Vehicles with multiple concurrent alerts produce multiple rows in `gold_vehicle_status`; vehicles without alerts produce a single row with `alert_type = NULL`.
- **Selective Atomic Refresh**: In each micro-batch, only vehicles present in the current batch have their Gold records refreshed atomically in SQLite within a transaction (`DELETE` old vehicle records, `INSERT` new evaluated records). Inactive vehicles remain untouched.
- **8 Critical Business, Safety & Sensor Alerts**:
  1. **Excessive Idling (`EXCESSIVE_IDLE`)**: Engine on (`engine_status == 1`) and stationary (`speed == 0`) continuously for $> 10\text{ minutes}$ ($600\text{ s}$). Identifies fuel waste and depot delays.
  2. **Overheating Critical (`OVERHEATING_CRITICAL`)**: Engine temperature $> 75^\circ\text{C}$ continuously for $> 60\text{ seconds}$ to flag imminent cooling/radiator failure.
  3. **Harsh Braking (`HARSH_BRAKING`)**: Consecutive event differential deceleration $\frac{\Delta\text{speed}}{\Delta t} < -15.0\text{ km/h/s}$.
  4. **Rapid Acceleration (`RAPID_ACCELERATION`)**: Consecutive event differential acceleration $\frac{\Delta\text{speed}}{\Delta t} > +12.0\text{ km/h/s}$.
  5. **Overspeeding (`OVERSPEEDING`)**: Sustained speed $> 110\text{ km/h}$ continuously for $\ge 5\text{ minutes}$ ($300\text{ s}$).
  6. **Ghost Towing / Rollaway (`GHOST_TOWING`)**: Engine off (`engine_status == 0`) while vehicle is moving (`speed > 5 km/h` or rapid GPS coordinate translation $> 5\text{ km/h}$). Flags theft, towing, or runaway vehicles.
  7. **Cold Engine Hard Acceleration (`COLD_ENGINE_HARD_ACCEL`)**: High speed ($> 60\text{ km/h}$) or rapid acceleration while engine temp $< 50^\circ\text{C}$ (cold coolant), identifying powertrain abuse.
  8. **Stuck / Frozen Sensor Anomaly (`STUCK_SENSOR_ANOMALY`)**: Perfectly static GPS coordinates or engine temperature reading (zero variance) across $N \ge 5$ moving events (`speed > 10 km/h`).
  9. **Thermal Spike at Idle (`THERMAL_SPIKE_AT_IDLE`)**: Rapid engine temperature rise ($\ge 5.0^\circ\text{C}$ in $\le 60\text{ s}$ or rate $\ge 0.08^\circ\text{C/s}$) while stationary and idling, signaling cooling fan clutch failure or radiator blockage.

---

## Getting Started

### 1. Environment Setup

```bash
# Copy template environment variables and update the configuration
cp .env.example .env.local

# Install in editable mode
pip install -e ".[dev]"
```

### 2. Local Database Initialization
The local SQLite database schema is defined in `config/setup_local_tables.sql` (the SQLite equivalent of `config/setup_tables.sql`), creating:
- `bronze_fleet_raw`
- `silver_fleet_quarantine` (DLQ)
- `silver_fleet_events`
- `gold_vehicle_status`

### 3. Running the Local Pipeline & Telemetry Generator

#### A. IoT Telemetry Generator CLI (`fleet-producer`)

The telemetry generator simulates connected vehicles emitting telemetry packets with realistic anomaly injection (terminal garbage, duplicate packets, late arrivals, and sensor drift). It can stream directly to Confluent Cloud Kafka or print events locally to stdout in dry-run mode.

```bash
# 1. Stream events to Confluent Cloud Kafka (5 events/s for 30s across 20 vehicles):
fleet-producer start --rate 5 --duration 30 --vehicles 20
# Or invoke via module:
python -m producer.simulate_iot start --rate 5 --duration 30

# 2. Dry-run mode (prints JSON payloads and anomaly labels to stdout without Kafka):
fleet-producer start --rate 2 --duration 10 --dry-run

# 3. Generate a quick sample batch of events:
fleet-producer sample --count 5 --vehicles 10

# 4. Stream indefinitely to a specific topic:
fleet-producer start --rate 10 --duration 0 --topic fleet.telemetry.raw
```

You can also use the Makefile shortcut:
```bash
make run-producer          # Runs fleet-producer start --rate 5 --duration 30
```

#### B. Streaming Pipeline Execution (`local_pipeline`)

The local pipeline ingests from either Confluent Cloud Kafka or the built-in offline rate generator:

```bash
# 1. Run local streaming pipeline connected to Kafka (default):
python -m local_pipeline.main --source kafka

# 2. Run local streaming pipeline with offline synthetic generator (no Kafka required):
python -m local_pipeline.main --source generator

# 3. Reset local database, lakehouse storage, and checkpoints before running:
python -m local_pipeline.main --source generator --reset

# 4. Check record counts and latest event timestamps across SQLite tables:
python -m local_pipeline.main --status

# 5. Run a specific streaming layer:
python -m local_pipeline.main --layer bronze --source generator
python -m local_pipeline.main --layer silver
python -m local_pipeline.main --layer gold
```

You can also use the Makefile shortcuts:
```bash
make run-local-pipeline    # Runs with Kafka source
make run-local-generator   # Runs with offline synthetic generator
make status-local-db       # Displays SQLite table counts
make reset-local-db        # Clears checkpoints and local SQLite database
```

---

### 4. Serving Layer & End-to-End Verification


```bash
# Start FastAPI (automatically routes queries to SQLite when running locally)
uvicorn api.main:app --reload --port 8000

# Query fleet status (retrieves live vehicle status from SQLite):
curl http://127.0.0.1:8000/fleet/status?limit=10

# Query health check (computes SLA lag against latest SQLite event timestamp):
curl http://127.0.0.1:8000/health

# Run test suite
pytest
```
