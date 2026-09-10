-- =============================================================================
-- SQLite Idempotent Table Initialization Script for Local Pipeline
-- Database target: data/fleet_local.db
-- Replicates the Unity Catalog Delta Lake Medallion Architecture locally
-- =============================================================================

-- 1. Bronze Layer Table: Append-only raw ingested events from Kafka / Generator
CREATE TABLE IF NOT EXISTS bronze_fleet_raw (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_payload TEXT NOT NULL,
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset INTEGER NOT NULL,
    kafka_timestamp TEXT NOT NULL,
    ingestion_timestamp TEXT NOT NULL,
    CONSTRAINT uq_bronze_kafka UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_bronze_ingestion_ts ON bronze_fleet_raw(ingestion_timestamp);

-- 2. Silver Quarantine Table: Dead Letter Queue (DLQ) for terminal garbage
CREATE TABLE IF NOT EXISTS silver_fleet_quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    raw_payload TEXT NOT NULL,
    error_reason TEXT NOT NULL,
    ingestion_timestamp TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_silver_quarantine_ts ON silver_fleet_quarantine(ingestion_timestamp);

-- 3. Silver Clean Table: Cleaned, deduplicated, and speed-clamped events
CREATE TABLE IF NOT EXISTS silver_fleet_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id TEXT NOT NULL,
    event_timestamp TEXT NOT NULL,
    latitude REAL,
    longitude REAL,
    speed_kph REAL NOT NULL,
    raw_speed_kph REAL NOT NULL,
    speed_clamped INTEGER NOT NULL,  -- 0 or 1 for boolean
    engine_temp_c REAL,
    engine_status INTEGER,
    odometer_km REAL,
    ingestion_timestamp TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_silver_events_vid_ts ON silver_fleet_events(vehicle_id, event_timestamp);
CREATE INDEX IF NOT EXISTS idx_silver_events_ingest_ts ON silver_fleet_events(ingestion_timestamp);

-- 4. Gold Table: Real-time latest vehicle status with multi-row alerts
CREATE TABLE IF NOT EXISTS gold_vehicle_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    vehicle_id TEXT NOT NULL,
    last_event_timestamp TEXT NOT NULL,
    latitude REAL,
    longitude REAL,
    current_speed_kph REAL NOT NULL,
    engine_temp_c REAL,
    engine_status INTEGER,
    alert_type TEXT,                  -- NULL if no active alerts, or alert enum name
    alert_details TEXT,               -- Context / trigger metrics description
    last_updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gold_vehicle_id ON gold_vehicle_status(vehicle_id);
CREATE INDEX IF NOT EXISTS idx_gold_alert_type ON gold_vehicle_status(alert_type);
CREATE INDEX IF NOT EXISTS idx_gold_last_event_ts ON gold_vehicle_status(last_event_timestamp);

-- 5. Pipeline Stream Metrics Table: Real-time PySpark micro-batch progress tracking
CREATE TABLE IF NOT EXISTS pipeline_stream_metrics (
    query_name TEXT PRIMARY KEY,
    batch_id INTEGER NOT NULL,
    num_input_rows INTEGER NOT NULL,
    input_rows_per_sec REAL NOT NULL,
    processed_rows_per_sec REAL NOT NULL,
    updated_at TEXT NOT NULL
);


