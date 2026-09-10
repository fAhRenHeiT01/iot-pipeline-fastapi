-- =============================================================================
-- Unity Catalog Idempotent Table & Volume Initialization Script
-- Catalog: fleet_iot | Schema: telemetry
-- =============================================================================

-- 1. Catalog & Schema Setup
CREATE CATALOG IF NOT EXISTS fleet_iot;
USE CATALOG fleet_iot;

CREATE SCHEMA IF NOT EXISTS fleet_iot.telemetry;
USE SCHEMA telemetry;

-- 2. Unity Catalog Volumes
-- Volume for Structured Streaming query checkpoints
CREATE VOLUME IF NOT EXISTS fleet_iot.telemetry.checkpoints
COMMENT 'Unity Catalog volume for Structured Streaming query checkpoints';

-- Volume for quarantined raw dumps and dead letter queue archives
CREATE VOLUME IF NOT EXISTS fleet_iot.telemetry.quarantine
COMMENT 'Unity Catalog volume for quarantined and rejected payload archives';

-- 3. Bronze Layer Table: Append-only raw ingested events
CREATE TABLE IF NOT EXISTS fleet_iot.telemetry.bronze_fleet_raw (
    raw_payload STRING NOT NULL,
    kafka_topic STRING NOT NULL,
    kafka_partition INT NOT NULL,
    kafka_offset BIGINT NOT NULL,
    kafka_timestamp TIMESTAMP NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.appendOnly' = 'true',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Raw immutable streaming telemetry payloads from Confluent Cloud Kafka';

-- 4. Silver Quarantine Table: Dead Letter Queue (DLQ) for terminal garbage
CREATE TABLE IF NOT EXISTS fleet_iot.telemetry.silver_fleet_quarantine (
    raw_payload STRING NOT NULL,
    error_reason STRING NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Quarantined terminal garbage events for audit and failure investigation';

-- 5. Silver Clean Table: Cleaned, deduplicated, and speed-clamped events
CREATE TABLE IF NOT EXISTS fleet_iot.telemetry.silver_fleet_events (
    vehicle_id STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    latitude DOUBLE,
    longitude DOUBLE,
    speed_kph DOUBLE NOT NULL,
    raw_speed_kph DOUBLE NOT NULL,
    speed_clamped BOOLEAN NOT NULL,
    engine_temp_c DOUBLE,
    engine_status INT,
    is_idle BOOLEAN NOT NULL,
    odometer_km DOUBLE,
    ingestion_timestamp TIMESTAMP NOT NULL
)
USING DELTA
-- PARTITIONED BY (DATE(event_timestamp))   # Delta Lake's auto-optimization features handle data layout efficiently without manual partitioning.
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Cleaned, deduplicated, and speed-clamped telemetry events with 15-min watermark';

-- 6. Gold Table: Real-time latest vehicle status with active alerts
CREATE TABLE IF NOT EXISTS fleet_iot.telemetry.gold_vehicle_status (
    vehicle_id STRING NOT NULL,
    last_event_timestamp TIMESTAMP NOT NULL,
    latitude DOUBLE,
    longitude DOUBLE,
    current_speed_kph DOUBLE NOT NULL,
    engine_temp_c DOUBLE,
    engine_status INT,
    alert_type STRING,
    alert_details STRING,
    last_updated_at TIMESTAMP NOT NULL
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Real-time vehicle status and active alerts updated via micro-batch MERGE/UPSERT';


