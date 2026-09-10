-- Databricks Unity Catalog Governance Policies & Access Control
-- Catalog: fleet_iot, Schema: telemetry

CREATE CATALOG IF NOT EXISTS fleet_iot;
CREATE SCHEMA IF NOT EXISTS fleet_iot.telemetry;

-- 1. Table Declarations
-- Bronze: Append-only raw ingested events
CREATE TABLE IF NOT EXISTS fleet_iot.telemetry.bronze_fleet_raw (
    raw_payload STRING,
    kafka_topic STRING,
    kafka_partition INT,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('delta.appendOnly' = 'true')
COMMENT 'Raw immutable streaming telemetry payloads from Confluent Kafka';

-- Silver Quarantine: Diverted malformed or missing key records
CREATE TABLE IF NOT EXISTS fleet_iot.telemetry.silver_fleet_quarantine (
    raw_payload STRING,
    error_reason STRING,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
COMMENT 'Quarantined terminal garbage events for audit and failure investigation';

-- Silver Clean: Validated, deduplicated, and clamped events
CREATE TABLE IF NOT EXISTS fleet_iot.telemetry.silver_fleet_events (
    vehicle_id STRING,
    event_timestamp TIMESTAMP,
    latitude DOUBLE,
    longitude DOUBLE,
    speed_kph DOUBLE,
    raw_speed_kph DOUBLE,
    speed_clamped BOOLEAN,
    engine_temp_c DOUBLE,
    engine_status INT,
    is_idle BOOLEAN,
    ingestion_timestamp TIMESTAMP
)
USING DELTA
PARTITIONED BY (DATE(event_timestamp))
COMMENT 'Cleaned, deduplicated, and speed-clamped telemetry events with 15-min watermark';

-- Gold Status: Real-time latest state per vehicle
CREATE TABLE IF NOT EXISTS fleet_iot.telemetry.gold_vehicle_status (
    vehicle_id STRING,
    last_event_timestamp TIMESTAMP,
    latitude DOUBLE,
    longitude DOUBLE,
    current_speed_kph DOUBLE,
    engine_temp_c DOUBLE,
    is_idle BOOLEAN,
    idle_duration_minutes DOUBLE,
    has_speed_alert BOOLEAN,
    last_updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Real-time vehicle status updated via micro-batch MERGE INTO';

-- 2. Role-Based Access Control (RBAC)
GRANT USAGE ON CATALOG fleet_iot TO `data_engineers`, `api_service_principal`;
GRANT USAGE ON SCHEMA fleet_iot.telemetry TO `data_engineers`, `api_service_principal`;

-- Service principal for FastAPI read-only access to Gold & Silver tables
GRANT SELECT ON TABLE fleet_iot.telemetry.gold_vehicle_status TO `api_service_principal`;
GRANT SELECT ON TABLE fleet_iot.telemetry.silver_fleet_events TO `api_service_principal`;

-- Data Engineers full management
GRANT ALL PRIVILEGES ON SCHEMA fleet_iot.telemetry TO `data_engineers`;

