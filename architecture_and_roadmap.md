# 1. Architecture & Data Flow

```
[IoT Telemetry CLI Generator]
        │ (SASL/SSL Plaintext / JSON payloads)
        ▼
[Confluent Cloud Kafka: fleet.telemetry.raw]
        │
        ▼
[Bronze: bronze_fleet_raw (Append-only Delta Lake)]
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
<br>

# 2. Data Cleaning Taxonomy & Resilience Rules
<br>

| Classification | Manifestation / Trigger | Handling Strategy | Target Storage / Field |
| :--- | :--- | :--- | :--- |
| **Terminal Garbage** | Corrupt JSON, `vehicle_id IS NULL`, unparseable ISO timestamp | Divert immediately; isolate from state stores | `silver_fleet_quarantine` |
| **Network Duplicates** | Identical `(vehicle_id, event_timestamp)` payloads | Deduplicate within watermark window | Dropped in Silver stage |
| **Out-of-Order / Late** | Payloads delayed between 15 and 45 minutes | Drop/divert if older than event-time watermark | Dropped beyond 15-min boundary |
| **Sensor Noise / Drift** | Speed < 0 or > 160 km/h; GPS coords out of range | Clamp speed; nullify GPS drift; flag QA issue | Kept in `silver_fleet_events` with warnings |
| **Backlog Bursts** | Multi-hour downtime recovery causing high ingest bursts | Throttled catchup batches (`maxOffsetsPerTrigger`) | Protects Spark JVM and Delta transactions |

<br>

# 3. End-to-End Repository Layout

```
fleet-realtime-lakehouse/
├── .github/workflows/ci.yml           # Automated lint, unit tests, and API contract checks
├── databricks.yml                     # Databricks Asset Bundle (DAB) declaration
├── pyproject.toml                     # Python dependencies, Ruff, and Pytest configuration
├── Makefile                           # Development task runner (lint, test, run-producer)
├── .env.example                       # Kafka and Databricks credentials template
│
├── config/
│   ├── pipeline_config.yaml           # Watermark windows, SLA targets, table paths
│   └── governance_policies.sql        # Unity Catalog RBAC grants and row filters
│
├── src/
│   ├── common/
│   │   ├── __init__.py
│   │   ├── config.py                  # Pydantic Settings management
│   │   └── logger.py                  # Structured JSON logging
│   │
│   ├── producer/                      # Kafka generator CLI
│   │   ├── __init__.py
│   │   ├── models.py                  # Pydantic event contracts
│   │   ├── generator.py               # Outlier, duplicate, and late event generation
│   │   └── cli.py                     # Typer CLI entrypoint
│   │
│   ├── pipeline/                      # Databricks Structured Streaming Core
│   │   ├── __init__.py
│   │   ├── schemas.py                 # PySpark StructTypes for Bronze and Silver
│   │   ├── bronze.py                  # Raw stream append to Delta
│   │   ├── silver.py                  # Quarantine split, watermarking, deduplication
│   │   ├── gold.py                    # Micro-batch MERGE & idle alert evaluation
│   │   └── listeners.py               # StreamingQueryListener emitting SLA metrics
│   │
│   └── api/                           # FastAPI Serving Layer
│       ├── __init__.py
│       ├── main.py                    # Application lifespans and routing
│       ├── dependencies.py            # Databricks SQL Serverless connector setup
│       ├── models.py                  # Response schemas (VehicleStatus, HealthCheck)
│       └── routes/
│           ├── fleet.py               # GET /fleet/status
│           ├── health.py              # GET /health (SLA verification)
│           └── metrics.py             # GET /metrics (Prometheus)
│
├── tests/
│   ├── conftest.py                    # Shared local SparkSession and mock clients
│   ├── fixtures/
│   │   └── sample_telemetry.json      # Valid, dirty, late, and duplicate payloads
│   ├── unit/
│   │   ├── test_generator.py          # Producer unit tests
│   │   ├── test_silver_cleansing.py   # PySpark quarantine & clamping tests
│   │   ├── test_silver_dedup.py       # Deduplication and watermark unit tests
│   │   ├── test_gold_alerts.py        # Idle state evaluation tests
│   │   └── test_api_routes.py         # FastAPI contract verification
│   └── integration/
│       ├── test_stream_microbatch.py  # trigger(availableNow=True) end-to-end run
│       └── test_api_e2e.py            # End-to-end API freshness tests
│
└── docs/
    └── architecture.md                # Design justification and trade-offs
```