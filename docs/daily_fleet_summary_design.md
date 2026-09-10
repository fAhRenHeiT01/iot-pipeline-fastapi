# Design Document: LLM-Based Daily Fleet Operational Summary

**Document Version**: 1.0  
**Status**: Approved / Ready for Review  
**Target System**: Fleet Real-time Lakehouse & IoT Pipeline  
**Target Audience**: Data Engineers, ML Engineers, Platform Architects, Fleet Operations Leadership  

---

## 1. Executive Summary & Objective

The **Fleet Real-time Lakehouse** currently processes high-throughput IoT telemetry through Bronze (raw Kafka ingest), Silver (cleansing, deduplication, 10-minute event-time watermarking, and quarantine DLQ), and Gold (real-time vehicle state, sliding-window calculations, and multi-row safety/mechanical alerts).

While the real-time Gold table (`gold_vehicle_status`) serves sub-minute operational dashboards and FastAPI endpoints (`GET /fleet/status`, `GET /health`), operations executives, depot dispatchers, and maintenance leads require a **synthesized Daily Fleet Briefing** delivered at 06:00 local time each morning.

This document sketches an **enterprise-grade, production-hardened LLM architecture** that transforms 24 hours of operational telemetry, safety infractions, and vehicle health events into an executive-ready daily briefing. It guarantees **100% mathematical accuracy**, eliminates hallucinations, ensures strict **data-freshness SLAs**, and operates reliably within enterprise cost and compliance boundaries.

---

## 2. Core Operational & Analytical Requirements

### 2.1 Daily Briefing Persona Needs
1. **Executive VP of Fleet Ops**: High-level KPIs (Fleet Availability %, Active vs. Idle utilization, fuel waste cost estimate, safety risk index, Day-over-Day trends).
2. **Depot Maintenance Supervisors**: Specific lists of high-risk vehicles requiring shop inspection (e.g., persistent `OVERHEATING_CRITICAL`, `THERMAL_SPIKE_AT_IDLE`, `STUCK_SENSOR_ANOMALY`).
3. **Safety & Compliance Officers**: High-severity driver infractions requiring coaching (`OVERSPEEDING`, `HARSH_BRAKING`, `RAPID_ACCELERATION`, `GHOST_TOWING`).

### 2.2 Functional Capabilities
- **Automated Morning Delivery**: Dispatched daily at 06:00 AM UTC via multi-channel endpoints (Email/Slack/FastAPI `/fleet/reports/daily`).
- **Deterministic Metric Grounding**: All numerical statements (counts, percentages, averages, financial estimates) must be mathematically provable against lakehouse tables.
- **Root-Cause Analysis & Actionable Directives**: The LLM must not just regurgitate numbers, but synthesize patterns (e.g., *"Depot B exhibited 64% of all excessive idling incidents, correlating with shift turnaround delays between 14:00 and 16:00"*).

---

## 3. Data-Freshness & Aggregation Guarantees

An LLM summary is only as good as the underlying data completeness. Generating reports from streaming IoT pipelines faces unique challenges: late-arriving telemetry, network disconnects, partition lag, and watermark cutoffs.

```
                    Day D Timeline (00:00:00 - 23:59:59 UTC)
├───────────────────────────────────────────────────────────────────────┤
                                                                        ▼ Day D Closes
                                                      00:00:00 UTC ──────┐
                                                                         │ 60-min Watermark & Re-try Buffer
                                                      01:00:00 UTC ◄─────┘ (Late-arriving packets flush)
                                                                         │
                                                                         ▼ Upstream Gate: SLA & Completeness Check
                                                      01:15:00 UTC ──────┤ (Kafka lag == 0, Watermark > 00:15)
                                                                         │
                                                                         ▼ Batch Gold Aggregation Run
                                                      01:30:00 UTC ──────┤ (Compute gold_fleet_daily_summary)
                                                                         │
                                                                         ▼ LLM Synthesis & Verification Gate
                                                      05:30:00 UTC ──────┤ (Deterministic Prompt + Claim Checker)
                                                                         │
                                                                         ▼ Executive Delivery (Email / Slack / API)
                                                      06:00:00 UTC ──────┘
```

### 3.1 Watermarking & Late-Arriving Data Policy
- **The Challenge**: The streaming Silver layer employs a 10-minute event-time watermark (`max_event_ts - 10 minutes`). Telemetry emitted at 23:59:50 UTC by a vehicle entering an underground depot may not reach Confluent Cloud Kafka until 00:12:00 UTC.
- **Guarantee**:
  - The aggregation window for Day $D$ is strictly $[D_{00:00:00\text{ UTC}}, D_{23:59:59\text{ UTC}}]$ based on **event_timestamp** (not ingestion timestamp).
  - The aggregation pipeline is scheduled with a **60-minute freeze buffer** (initiating at 01:00:00 UTC). This allows all in-flight buffers, Kafka partitions, and micro-batch triggers to flush past the watermark boundary.
  - Payloads delayed $> 60\text{ minutes}$ are categorized as anomalous backfills and accounted for in idempotency re-runs.

### 3.2 Upstream Freshness & Readiness Gates (Pre-flight Sensors)
Before triggering the daily aggregation and LLM pipeline, an automated orchestrator sensor (Airflow / Databricks Workflow) validates three invariant gates:
1. **Kafka Partition Lag Gate**: Consumer lag across all partitions for `fleet.telemetry.raw` must be $< 100\text{ messages}$ (pipeline is near real-time).
2. **Stream Watermark Gate**: The PySpark Structured Streaming engine watermark progress must satisfy $\text{current\_watermark} \ge D_{+1\text{ day } 00:15:00\text{ UTC}}$.
3. **Quarantine / DLQ Threshold Gate**: The error ratio in `silver_fleet_quarantine` for date $D$ must not exceed $2.0\%$ of total bronze ingress. If exceeded, a `DATA_QUALITY_DEGRADATION` alert is flagged in the report metadata.

### 3.3 Storage Layer Distinction: Real-Time Gold vs. Daily Rollup Gold
- **Current Real-Time Gold (`gold_vehicle_status`)**: Represents the *latest point-in-time state* per vehicle (upserted via micro-batch `MERGE INTO`). It cannot provide cumulative 24-hour distance, historical thermal cycles, or count total harsh-braking incidents across the day.
- **Enterprise Extension: Daily Rollup Gold (`gold_fleet_daily_summary`)**:
  - A deterministic daily batch job executes at 01:15 UTC over `silver_fleet_events` and historical Gold alert records.
  - Aggregates metrics by `vehicle_id` and rolls up to `fleet_daily_metrics`.
  - **Schema Contract**:
    ```sql
    CREATE TABLE IF NOT EXISTS fleet_iot.telemetry.gold_fleet_daily_summary (
        report_date DATE NOT NULL,
        fleet_size INT NOT NULL,
        active_vehicles INT NOT NULL,
        total_km_driven DOUBLE NOT NULL,
        total_engine_hours DOUBLE NOT NULL,
        total_idle_hours DOUBLE NOT NULL,
        fleet_idle_ratio DOUBLE NOT NULL,
        estimated_idle_fuel_wasted_liters DOUBLE NOT NULL,
        total_alerts_count INT NOT NULL,
        alerts_by_type MAP<STRING, INT> NOT NULL,  -- e.g. {'EXCESSIVE_IDLE': 142, 'OVERHEATING_CRITICAL': 8, ...}
        critical_vehicles_json STRING NOT NULL,    -- Ranked list of top 10 most degraded vehicles with telemetry facts
        created_at TIMESTAMP NOT NULL
    ) USING DELTA
    PARTITIONED BY (report_date);
    ```
- **Idempotency Guarantee**: Partition overwrite ensures rerunning the batch job for date $D$ produces byte-for-byte identical analytical facts without duplicates.

---

## 4. System Architecture

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       MEDALLION DATA LAKEHOUSE                                         │
│                                                                                                        │
│   ┌────────────────────────┐        ┌─────────────────────────┐        ┌───────────────────────────┐   │
│   │   bronze_fleet_raw     │───────▶│   silver_fleet_events   │───────▶│   gold_vehicle_status     │   │
│   │   (Raw Append Kafka)   │        │   (Cleaned, Deduplicated)│       │   (Real-time State)       │   │
│   └────────────────────────┘        └───────────┬─────────────┘        └─────────────┬─────────────┘   │
└─────────────────────────────────────────────────┼────────────────────────────────────┼─────────────────┘
                                                  │                                    │
                                                  ▼                                    ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                        TIER 1: DETERMINISTIC PRE-AGGREGATION & DATA QUALITY GATE                       │
│                                                                                                        │
│  1. Pre-flight Freshness Sensor: Verify Kafka lag == 0 & Watermark >= T + 15m                          │
│  2. PySpark Daily Aggregator: Run at 01:15 UTC                                                         │
│     - Compute Fleet KPIs: Availability %, Distance, Idle Hours, Fuel Waste                             │
│     - Group Alerts: 8 critical rules (Overspeeding, Overheating, Ghost Towing, etc.)                   │
│     - Rank Top 10 Degraded Vehicles with telemetry anomaly context                                     │
│  3. Persist to Delta Table: gold_fleet_daily_summary (Partitioned by report_date)                      │
│  4. Extract Compact JSON Fact Manifest (~3 - 5 KB)                                                    │
└─────────────────────────────────────────────────┬──────────────────────────────────────────────────────┘
                                                  │
                                                  ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                        TIER 2: ENTERPRISE LLM ORCHESTRATION & SYNTHESIS LAYER                          │
│                                                                                                        │
│  ┌──────────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ System Prompt & Instruction Invariants:                                                          │  │
│  │ - ZERO MATH POLICY: Do not calculate or extrapolate any numerical values.                        │  │
│  │ - FACT GROUNDING: Cite only metrics present in the JSON manifest.                                │  │
│  │ - SCHEMA ENFORCEMENT: Enforce Pydantic Output Model (Executive, Maintenance, Safety, Actions)   │  │
│  └──────────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                 │                                                      │
│                                                 ▼                                                      │
│  ┌──────────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ Enterprise LLM Engine (Private Endpoint / Zero Data Retention):                                  │  │
│  │ (Claude 3.5 Sonnet / GPT-4o / Google Gemini 1.5 Pro / Databricks Foundation Model Serving)        │  │
│  └──────────────────────────────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┬──────────────────────────────────────────────────────┘
                                                  │
                                                  ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                        TIER 3: AUTOMATED GUARDRAIL & VERIFICATION GATE                                 │
│                                                                                                        │
│  ┌───────────────────────────────────────────────┐     FAIL (Hallucinated / Mismatched Metric)         │
│  │ Metric Claim Cross-Validator (Regex Parser)   │──────────────────────────────┐                      │
│  │ - Extract all numbers/percentages from output │                              ▼                      │
│  │ - Assert every number exists in JSON Manifest │              ┌──────────────────────────────────┐   │
│  └───────────────────────┬───────────────────────┘              │ Deterministic Fallback Template  │   │
│                          │ PASS                                 │ (Jinja2 Markdown Engine)         │   │
│                          ▼                                      └────────────────┬─────────────────┘   │
│  ┌───────────────────────────────────────────────┐                               │                     │
│  │ Persist Narrative: gold_fleet_daily_briefings │◀──────────────────────────────┘                     │
│  └───────────────────────┬───────────────────────┘                                                     │
└──────────────────────────┼─────────────────────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                        TIER 4: MULTI-CHANNEL DISTRIBUTION & SERVING                                    │
│                                                                                                        │
│  ├── FastAPI Serving: GET /fleet/reports/daily?date=YYYY-MM-DD                                         │
│  ├── Operations Dashboard: Web Component / Executive KPI Portal                                        │
│  ├── Slack Dispatcher: #fleet-ops-daily-briefing                                                       │
│  └── Email Gateway: Executive HTML Digest to Regional Directors                                        │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Architectural Options & Fact-Based Decisions

| Strategy Option | Technical Description | Strengths | Fatal Flaws / Trade-offs | Decision |
| :--- | :--- | :--- | :--- | :--- |
| **Option 1: Dynamic Text-to-SQL at 06:00** | LLM receives lakehouse table schemas, dynamically writes SQL to calculate metrics, and answers prompts. | Ad-hoc flexibility; zero ETL development. | **Fatal**: High probability of syntax/semantic SQL errors; nondeterministic calculations; high query runtime on billions of rows; massive security attack surface. | **REJECTED** |
| **Option 2: Raw Context Dump (RAG over Raw Events)** | Retrieve hundreds of thousands of raw Silver/Gold rows and inject into long-context window (e.g., 1M tokens). | Preserves all granular data; simple retrieval pipeline. | **Fatal**: Extreme API cost ($10–$50 per run); LLMs are notoriously inaccurate at aggregations and math; attention dilution misses critical outliers. | **REJECTED** |
| **Option 3: Domain-Specific Fine-Tuned LLM** | Train/fine-tune an open-source model (Llama 3 8B) on historical fleet summary logs. | Tailored terminology and style; low per-inference compute cost. | **Fatal**: Ineffective for daily dynamic facts; fine-tuned models still hallucinate numbers; immense maintenance overhead when business rules change. | **REJECTED** |
| **Option 4: Two-Tier Deterministic Aggregation + Grounded Context Injection (Pydantic Schema + Claim Checker)** | PySpark computes exact mathematical KPIs into a Delta table. A compact JSON manifest (~4 KB) is passed to an enterprise LLM with strict numeric anchoring and guardrails. | **100% mathematical accuracy**; fast inference (< 5s); tiny token cost (< $0.02/report); deterministic fallback guarantee; full auditability. | **ACCEPTED (Enterprise Standard)** |

### Detailed Decision Rationale for Option 4
1. **Separation of Concerns**: Distributed query engines (PySpark / Delta Lake) excel at parallel aggregation over billions of records. LLMs excel at linguistic synthesis, pattern contextualization, and narrative reasoning. We do not use the LLM as a calculator.
2. **Deterministic Token Footprint**: Regardless of whether the fleet scales from 100 to 100,000 vehicles, the pre-aggregation stage reduces the input to a fixed-size JSON payload (Fleet KPIs + Top 10 Anomaly Outliers). Token consumption remains $O(1)$.
3. **Auditable Lineage**: Every number cited in the narrative traces back directly to a column in `gold_fleet_daily_summary`.

---

## 6. Prompt Engineering & Structured Context Contract

### 6.1 Structured Input Manifest (`FactManifest` JSON)
```json
{
  "report_date": "2026-09-09",
  "fleet_overview": {
    "total_vehicles": 250,
    "active_vehicles": 238,
    "utilization_pct": 95.2,
    "total_distance_km": 48210.5,
    "total_engine_hours": 2140.2,
    "total_idle_hours": 385.4,
    "idle_ratio_pct": 18.01,
    "estimated_fuel_wasted_liters": 1156.2,
    "fuel_waste_cost_usd": 1503.06
  },
  "safety_and_mechanical_alerts": {
    "total_alerts": 76,
    "breakdown": {
      "EXCESSIVE_IDLE": 34,
      "HARSH_BRAKING": 18,
      "RAPID_ACCELERATION": 11,
      "OVERSPEEDING": 6,
      "OVERHEATING_CRITICAL": 4,
      "THERMAL_SPIKE_AT_IDLE": 2,
      "STUCK_SENSOR_ANOMALY": 1,
      "GHOST_TOWING": 0
    }
  },
  "top_critical_vehicles": [
    {
      "vehicle_id": "VH-8821",
      "depot": "Depot-North",
      "critical_issues": [
        {"alert": "OVERHEATING_CRITICAL", "details": "Engine temp reached 108.4 C for 240s"},
        {"alert": "THERMAL_SPIKE_AT_IDLE", "details": "Temp rose 9.2 C in 45s while stationary"}
      ],
      "recommended_action": "Immediate cooling system inspection; potential radiator fan failure."
    },
    {
      "vehicle_id": "VH-1044",
      "depot": "Depot-East",
      "critical_issues": [
        {"alert": "OVERSPEEDING", "details": "Speed reached 134 km/h; sustained > 110 km/h for 12 mins"},
        {"alert": "HARSH_BRAKING", "details": "Deceleration -18.2 km/h/s recorded 4 times"}
      ],
      "recommended_action": "Driver safety coaching required; inspect brake pads."
    }
  ],
  "benchmark_comparisons": {
    "day_over_day_idle_change_pct": +2.4,
    "week_over_week_alerts_change_pct": -12.5
  }
}
```

### 6.2 Structured Output Schema (Pydantic Model)
```python
from pydantic import BaseModel, Field

class ExecutiveSummary(BaseModel):
    headline: str = Field(description="One-sentence executive summary of fleet performance.")
    operational_health_score: str = Field(description="Rating: Excellent, Good, Fair, or Critical.")
    key_takeaways: list[str] = Field(description="3-4 bullet points highlighting key operational highlights.")

class MaintenancePriorities(BaseModel):
    urgent_inspections: list[dict[str, str]] = Field(
        description="Vehicles requiring immediate shop pull with vehicle_id, fault, and recommendation."
    )
    sensor_health_notes: str = Field(description="Observations on frozen/stuck telemetry sensors.")

class SafetyAndDriverBehavior(BaseModel):
    safety_incident_summary: str = Field(description="Analysis of overspeeding, harsh braking, and rapid acceleration.")
    coaching_recommendations: list[str] = Field(description="Targeted safety intervention points.")

class OperationalEfficiency(BaseModel):
    idling_and_fuel_impact: str = Field(description="Narrative on fuel waste, idling patterns, and financial impact.")
    utilization_assessment: str = Field(description="Analysis of active vs idle fleet capacity.")

class DailyFleetBriefing(BaseModel):
    report_date: str
    executive_summary: ExecutiveSummary
    operational_efficiency: OperationalEfficiency
    maintenance_priorities: MaintenancePriorities
    safety_and_driver_behavior: SafetyAndDriverBehavior
```

---

## 7. Hallucination Mitigation & Output Verification

To meet enterprise compliance in industrial IoT, the system implements a **three-tier anti-hallucination defense**:

### 7.1 Tier 1: Zero-Temperature & Explicit Invariant Prompts
- Model temperature is set to `0.0` (purely deterministic greedy decoding).
- System instruction enforces:
  > *"You are an industrial fleet audit AI. You must NEVER compute, estimate, or modify any numeric value. Every single metric, percentage, currency figure, and vehicle ID mentioned in your output MUST be an exact verbatim match to an entry in the provided JSON fact manifest. If an operational metric is absent from the input, state that it was not tracked. Violating this rule will cause the summary to be rejected."*

### 7.2 Tier 2: Automated Numeric Claim Cross-Validator
Before the generated response is persisted or dispatched, an automated Python validation filter parses the output:
1. **Regex Extraction**: Extracts all numeric entities, percentages, currency symbols, and `VH-XXXX` identifiers from the LLM text response.
2. **Fact Intersection Assert**: Cross-checks extracted tokens against the set of valid numbers in `FactManifest`.
3. **Rejection Threshold**: If any numeric claim in the LLM response is not present in the input manifest (e.g., the LLM hallucinated *"fuel waste was $1,800"* when the JSON stated `1503.06`), the generation is immediately invalidated.

### 7.3 Tier 3: Deterministic Fallback Engine (Jinja2)
If the LLM call times out, fails schema validation, or fails the claim validator, the system automatically falls back to a **pre-compiled Jinja2 Markdown template**. The template renders the exact same JSON metrics into a structured briefing without prose generation. **Operations never miss their morning report.**

---

## 8. Risks & Mitigation Strategies Matrix

| # | Risk Category | Specific Failure Scenario | Impact | Mitigation Strategy |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **Hallucination / Fact Fabrication** | LLM generates plausible-sounding vehicle IDs (`VH-9999`) or invents fuel loss figures not in the Gold table. | High: False maintenance dispatches; loss of executive trust. | **Numeric Claim Cross-Validator** + Pydantic strict schema + Zero temperature + Jinja2 deterministic fallback. |
| **2** | **Late Telemetry & Pipeline Lag** | Upstream Kafka lag or network drop delays 30% of night-shift telemetry until 04:00 AM. | High: Daily report undercounts mileage and incidents. | **60-min Watermark Buffer** + Pre-flight Sensor checking Kafka lag $< 100$ and stream progress before starting summary job. |
| **3** | **Cost & Token Window Explosion** | Fleet expands to 25,000 vehicles, causing prompt context to exceed token limits and incur high API costs. | Medium: Financial waste; context truncation errors. | **$O(1)$ Hierarchical Pre-Aggregation**: Spark computes fleet totals and filters only Top 10 anomaly vehicles for LLM prompt context (~4 KB). |
| **4** | **Vendor Outage / API Rate Limiting** | Commercial LLM provider experiences morning outage or rate-limiting (429 / 503). | High: 06:00 AM briefing delivery fails. | **Exponential backoff retry (3x)** + Circuit Breaker falling back immediately to deterministic Jinja2 template. |
| **5** | **Data Privacy & Driver PII** | Driver names, personal phone numbers, or residential addresses leak into prompt payloads. | Critical: GDPR / CCPA and union compliance violation. | **Pseudonymization at Silver/Gold layer**: Only vehicle IDs (`VH-XXXX`) and anonymized depot tags are passed to the LLM. No PII ingested into prompt. |
| **6** | **Schema Drift / Upstream Evolution** | New alert type added to Gold layer (e.g. `BATTERY_DRAIN_CRITICAL`) breaks LLM prompt context. | Medium: Model misinterprets or ignores new alerts. | **Dynamic Alert Mapping**: Alert counts stored as `MAP<STRING, INT>` in Delta and injected dynamically into prompt manifest. |

---

## 9. Rollout Plan & Operational Verification

1. **Phase 1: Shadow Pipeline (2 Weeks)**
   - Run daily batch aggregation at 01:15 UTC alongside real-time streaming.
   - Execute LLM briefing generation silently; store outputs in `fleet_iot.telemetry.gold_fleet_daily_briefings`.
   - Run automated Claim Validator on all outputs; calculate pass rate (Target: $> 99.5\%$).
2. **Phase 2: Pilot User Group (1 Week)**
   - Deliver briefing via Slack channel `#fleet-ops-pilot` to 3 depot managers.
   - Collect human-in-the-loop scoring on readability, utility of maintenance advice, and actionability.
3. **Phase 3: General Production Availability**
   - Activate FastAPI endpoint: `GET /fleet/reports/daily?date=YYYY-MM-DD`.
   - Schedule automated 06:00 AM executive email and Slack broadcast.

