# Fleet Real-time Lakehouse & IoT Pipeline

High-throughput, resilient IoT Fleet Telemetry Lakehouse featuring real-time Medallion streaming (Bronze, Silver, Gold), automated safety & mechanical anomaly detection, FastAPI operational serving, and an LLM-powered executive reporting architecture.

---

## System Architecture

```
[IoT Telemetry CLI Generator]
        │ (SASL/SSL JSON payloads)
        ▼
[Confluent Cloud Kafka: fleet.telemetry.raw]
        │
        ▼
[Bronze: bronze_fleet_raw] ── (WAL Offset Checkpointing & Idempotent Append)
        │
        ├───────────────────────────────────────────────────────┐
        ▼ (Terminal Garbage Quarantine)                         ▼
[Silver DLQ: silver_fleet_quarantine]           [Silver: silver_fleet_events]
(Malformed JSON, missing IDs, bad ts)           (10-15m Watermark, Dedup [vid, ts], Clamped speed)
                                                                │
                                                                ▼ (Sliding Window & Multi-row Alerts)
                                                [Gold: gold_vehicle_status]
                                                (8 Telemetry & Safety Alert Rules, Atomic UPSERT)
                                                                │
                                                ┌───────────────┴───────────────┐
                                                ▼                               ▼
                                    [FastAPI Serving Layer]       [LLM Daily Summary]
                                    • GET /fleet/status           • 06:00 UTC Operational Briefing
                                    • GET /health (SLA < 120s)    • Guardrails & Fallback
                                    • GET /metrics (Prometheus)   • (See docs/daily_fleet_summary_design.md)
```

### Watermarking & Windowing Strategy
- Enforce a 60-minute event-time watermark on the Bronze-to-Silver streaming pipeline. Drop late-arriving events that exceed the window directly to prevent state store unbounded growth.
- Evaluate real-time vehicle alerts (such as excessive idling $>10$ minutes) against the continuously updated live Gold table (gold_vehicle_status) with **$10$ minutes sliding window** using micro-batch upserts (MERGE INTO).
- Decouple downstream analytical aggregations (e.g., the Daily Fleet Summary) from the streaming watermark by implementing them as scheduled batch jobs querying settled Gold/Silver snapshots rather than maintaining divergent streaming watermarks. The **60-minute event-time watermark** will ensure delayed events are passed-through to Silver Layer for correct aggregation of **Daily Fleet Summary**

### Failure Recovery & Huge Backlog Handling
- Over-partition Kafka topics to a future-proof scale factor (e.g., 16 to 32 partitions) to decouple broker topology from initial cluster sizing.
```
Starting with more Kafka partitions than Spark executors is highly recommended.

Normal Traffic: 
Spark handles this gracefully. If you have 20 Kafka partitions and 5 Spark executors, Spark will naturally assign 4 partitions (tasks) to each executor.

Sudden Backlog: 
You can seamlessly scale up your Spark cluster to 20 executors. Spark will instantly shift to assigning 1 partition per executor, maximizing your cluster's parallel CPU power without any Kafka configuration changes or data reshuffling.
```

- Enforce deterministic streaming backpressure purely through Spark Structured Streaming’s `maxOffsetsPerTrigger` configuration in the Kafka read stream.

---

## Medallion Data Pipeline

The pipeline implements the Medallion architecture with parallel execution targets: local execution via PySpark & SQLite/Parquet (`src/local_pipeline/`), and enterprise cloud execution via Databricks Structured Streaming, Delta Lake, and Unity Catalog (`src/pipeline/` & `databricks.yml`).

### 1. Bronze Layer (Raw Ingestion)
- **Role**: Append-only ingestion capturing raw telemetry payloads with Kafka message metadata (topic, partition, offset, timestamp).
- **Guarantees**: Write-Ahead Log (WAL) checkpointing for exactly-once offset advancement; composite uniqueness on `(kafka_topic, kafka_partition, kafka_offset)` ensures idempotent writes across retries.
- **Implementations**:
  - **Local**: `src/local_pipeline/bronze.py` writes micro-batches to local Parquet storage (`data/lakehouse/bronze`) and SQLite table `bronze_fleet_raw`.
  - **Databricks**: `src/pipeline/bronze.py` appends to Delta table `fleet_iot.telemetry.bronze_fleet_raw` with auto-compaction enabled (`config/setup_tables.sql`).

### 2. Silver Layer (Cleansing, DLQ & Deduplication)
- **Role**: Validates payloads, filters terminal garbage, handles sensor drift, and deduplicates network bursts.
- **Dead Letter Queue (DLQ)**: Records with malformed JSON, missing `vehicle_id`, or unparseable timestamps divert immediately to `silver_fleet_quarantine` without stalling the stream.
- **Stateful Watermarking & Dedup**: Enforces a 10–15 minute event-time watermark (`max_event_ts - watermark`) and deduplicates across `(vehicle_id, event_timestamp)`.
- **Sensor Clamping**: Normalizes vehicle speed to physical bounds $[0.0, 160.0]\text{ km/h}$, setting `speed_clamped = true` while preserving `raw_speed_kph` for audits.
- **Implementations**:
  - **Local**: `src/local_pipeline/silver.py` applies partition sorting (`sortWithinPartitions`) for Parquet skipping and indexes SQLite table `silver_fleet_events`.
  - **Databricks**: `src/pipeline/silver.py` streams clean events to `fleet_iot.telemetry.silver_fleet_events` with Delta Lake auto-optimization.

### 3. Gold Layer (State Aggregation & Alert Engine)
- **Role**: Computes latest vehicle status, tracks sliding-window telemetry, and generates multi-row business, mechanical, and safety alerts.
- **Multi-Row Alert Schema**: Vehicles with active alerts output explicit `alert_type` and `alert_details` records. Vehicles operating normally output a single status row with `alert_type = NULL`.
- **Implementations**:
  - **Local**: `src/local_pipeline/gold.py` queries a 10-minute historical event window, executes a PySpark `applyInPandas` UDF (`local_pipeline/alerts.py`), and applies selective atomic refresh in SQLite table `gold_vehicle_status`.
  - **Databricks**: `src/pipeline/gold.py` executes micro-batch `MERGE INTO` on `fleet_iot.telemetry.gold_vehicle_status` keyed by `vehicle_id`.

#### Evaluated Telemetry & Safety Alerts
| Alert Type | Trigger Condition | Operational Impact |
| :--- | :--- | :--- |
| **`EXCESSIVE_IDLE`** | Engine running (`engine_status == 1`), speed $= 0$ for $> 10$ min | Fuel waste & depot turnaround delay |
| **`OVERHEATING_CRITICAL`** | Engine temperature $> 75^\circ\text{C}$ continuously for $> 60$ s | Imminent cooling system or head gasket failure |
| **`HARSH_BRAKING`** | Differential deceleration $\frac{\Delta\text{speed}}{\Delta t} < -15.0\text{ km/h/s}$ | Unsafe driving & excessive brake wear |
| **`RAPID_ACCELERATION`** | Differential acceleration $\frac{\Delta\text{speed}}{\Delta t} > +12.0\text{ km/h/s}$ | Aggressive driving & transmission stress |
| **`OVERSPEEDING`** | Sustained speed $> 110\text{ km/h}$ continuously for $\ge 5$ min | Speed limit infraction & road safety risk |
| **`GHOST_TOWING`** | Engine off (`engine_status == 0`), vehicle moving ($> 5\text{ km/h}$) | Vehicle theft, rollaway, or unauthorized towing |
| **`COLD_ENGINE_HARD_ACCEL`** | Speed $> 60\text{ km/h}$ or hard acceleration while engine $< 50^\circ\text{C}$ | Powertrain abuse & cylinder scoring |
| **`STUCK_SENSOR_ANOMALY`** | Zero variance in GPS or temp across $N \ge 5$ moving events | Sensor freeze, telemetry glitch, or wire disconnect |
| **`THERMAL_SPIKE_AT_IDLE`** | Temperature rise $\ge 5^\circ\text{C}$ in $\le 60$ s while stationary and idling | Cooling fan clutch failure or radiator blockage |

---

## LLM-Based Daily Fleet Summary (Design)

The system design defines an automated **Daily Fleet Operational Summary** generated each morning at 06:00 UTC from the Medallion Gold layer for fleet operations executives and depot maintenance leads.

- **Two-Tier Architecture**: Deterministic PySpark batch job pre-aggregates fleet metrics into a partitioned Delta table (`gold_fleet_daily_summary`). A compact ~4 KB JSON fact manifest is then synthesized by an enterprise LLM (e.g., Gemini 1.5 Pro, GPT-4o, Claude 3.5 Sonnet) with zero-math invariants.
- **Freshness & Watermark Guarantees**: Scheduled at 01:15 UTC with a 60-minute freeze buffer to allow the 10-minute event-time watermark to flush late arrivals. Pre-flight sensors verify Kafka consumer lag $< 100$ and DLQ error ratios $< 2\%$.
- **Anti-Hallucination Guardrails**: Features a zero-math prompt policy, an automated Regex Claim Cross-Validator matching output numbers to source facts, and a deterministic Jinja2 fallback template in case of model timeouts or API outages.
- **Multi-Channel Distribution**: Dispatched via FastAPI (`GET /fleet/reports/daily`), Slack `#fleet-ops-daily-briefing`, and executive email digests.

📄 **Full Technical Design**: See [docs/daily_fleet_summary_design.md](/docs/daily_fleet_summary_design.md) for full architecture diagrams, schema contracts, Pydantic models, and risk mitigation strategies.

---

## Serving Layer & API Endpoints

Built on FastAPI (`src/api/`), connecting directly to the local database or Databricks SQL Serverless in production (`src/api/dependencies.py`):

| Endpoint | Method | Description | SLA / Behavior |
| :--- | :--- | :--- | :--- |
| `/fleet/status` | `GET` | Current vehicle status, locations, metrics, and active alerts | Pagination via `limit` / `offset` |
| `/health` | `GET` | End-to-end freshness health check | Validates `now - max(last_event_ts) < 120 s` |
| `/metrics` | `GET` | Prometheus telemetry metrics | Micro-batch rates, rows/s, and pipeline lag |

---

## Getting Started

### 1. Environment Setup

```bash
# Clone and configure environment variables
cp .env.example .env.local

# Install package dependencies in editable mode
pip install -e ".[dev]"
```

### 2. Database Initialization
Initialize schema tables (`bronze_fleet_raw`, `silver_fleet_quarantine`, `silver_fleet_events`, `gold_vehicle_status`):
- **Local SQLite**: Handled automatically via `config/setup_local_tables.sql`.
- **Databricks Unity Catalog**: Executed via `config/setup_tables.sql`.

### 3. Running Telemetry Producer & Streaming Pipeline

#### A. IoT Telemetry Generator CLI (`fleet-producer`)
Simulates multi-vehicle streaming telemetry with realistic anomaly injection (corrupt payloads, sensor drift, late arrivals, duplicates):

```bash
# Stream to Confluent Cloud Kafka (5 events/s for 30s across 20 vehicles):
fleet-producer start --rate 5 --duration 30 --vehicles 20

# Dry-run mode (output to stdout without Kafka):
fleet-producer start --rate 2 --duration 10 --dry-run

# Shortcut via Makefile:
make run-producer
```

#### B. Streaming Pipeline (`local_pipeline`)
Ingests from Confluent Cloud Kafka or the offline synthetic generator:

```bash
# Run streaming pipeline with Kafka source:
python -m local_pipeline.main --source kafka

# Run streaming pipeline with offline synthetic generator:
python -m local_pipeline.main --source generator

# Run individual pipeline layers:
python -m local_pipeline.main --layer bronze --source generator
python -m local_pipeline.main --layer silver
python -m local_pipeline.main --layer gold

# Inspect table counts and timestamps:
python -m local_pipeline.main --status

# Reset checkpoints and database:
python -m local_pipeline.main --source generator --reset
```

### 4. Serving Layer & Verification

```bash
# Start FastAPI serving layer
uvicorn api.main:app --reload --port 8000

# Query live vehicle status
curl http://127.0.0.1:8000/fleet/status?limit=10

# Verify SLA freshness lag (< 120s)
curl http://127.0.0.1:8000/health

# Run test suite
pytest
```

---

## Repository Structure

```
fleet-realtime-lakehouse/
├── config/
│   ├── pipeline_config.yaml            # Watermarks, SLAs, and table identifiers
│   ├── setup_local_tables.sql          # SQLite local lakehouse schema
│   └── setup_tables.sql                # Databricks Unity Catalog Delta schema
├── databricks.yml                      # Databricks Asset Bundle (DAB) declaration
├── docs/
│   └── daily_fleet_summary_design.md   # LLM Daily Summary architecture & guardrails
├── src/
│   ├── api/                            # FastAPI application and endpoint routers
│   ├── common/                         # Configuration settings and structured logger
│   ├── local_pipeline/                 # Local PySpark & SQLite Medallion pipeline
│   ├── pipeline/                       # Databricks Structured Streaming & Delta MERGE
│   └── producer/                       # IoT telemetry generator and Typer CLI
└── tests/                              # Unit, integration, and contract tests
```
