# F1-pipeline-poc
## Overview

This repo delivers an **end-to-end MVP data platform** for 2025 Formula 1 analytics built on:
**AWS Lambda → S3 → Fivetran → Snowflake → dbt (STG/INT/MARTS) → Power BI**.

**Goals**
- Automate ingest from OpenF1 (sessions, drivers, laps, stints, pit) on race weekends.
- Land immutable **csv.gz** files in S3 by **year/month/day**.
- Load to Snowflake **RAW** via Fivetran S3 connector (zero-maintenance ELT).
- Transform with **dbt** into curated **dimensions** and **facts** for analytics.
- Power **two dashboards** (Race Overview & Pit Strategy) in Power BI.

**Data Flow**

OpenF1 API
|
v
AWS Lambda ─(csv.gz)→ S3: s3://f1-etl/raw/{table}/year=YYYY/month=MM/day=DD/{table}.csv.gz
|
v
Fivetran (S3 Files) → Snowflake: F1_ANALYTICS_DEV.RAW
|
v
dbt (Cloud/CLI) → STAG (stg/int views) → PROD (marts tables)
|
v
Power BI (Import) → Race Overview / Pit Strategy


**What’s in this repo**
- `lambda/` — Lambda app to fetch **latest session** + CSV endpoints and write to S3.
- `fivetran/` — S3 connector mapping notes (regex → table names).
- `dbt_openf1/`
  - `models/stg_openf1/` — clean staging views (1:1 with RAW).
  - `models/int_openf1/` — enriched views (lap/pace, pit joins, stint rollups).
  - `models/marts_openf1/`  
    - `dims/` — `dim_meeting`, `dim_session`, `dim_driver`  
    - `facts/` — `fct_race_summary`, `fct_pit_stop`, `fct_stint`
  - `macros/`, `seeds/`, `tests/` — project plumbing & quality.
- `powerbi/` — (optional) PBIX/template, theme, documentation.

**Environments (Snowflake)**
- `F1_ANALYTICS_DEV.RAW` — Fivetran landing tables  
- `F1_ANALYTICS_DEV.STAG` — dbt **stg**/**int** (views)  
- `F1_ANALYTICS_DEV.PROD` — dbt **marts** (tables)

**Data Model (snapshot)**
- **dim_meeting** (PK: `meeting_key`) — weekend/circuit attributes  
- **dim_session** (PK: `session_key`, FK: `meeting_key`) — session metadata  
- **dim_driver** (PK: `driver_sk` = surrogate of `session_key + driver_number`) — identity/team/color  
- **fct_race_summary** (grain: driver-session) — pace (best/median/σ), pits (count/avg/best), stints (count/compounds)  
- **fct_pit_stop** (grain: pit stop) — per-stop metrics, FK `driver_sk`  
- **fct_stint** (grain: stint) — stint windows & compounds, FK `driver_sk`

**Quickstart**
1. **Lambda**: set env vars (`API_BASE`, `S3_BUCKET`, `S3_PREFIX=raw`, `YEAR=2025`), deploy, and schedule via EventBridge.
2. **Fivetran**: S3 Files connector → Bucket `f1-etl`, Base path `raw`, Role ARN; add mappings:  
   `^sessions/.+\.csv\.gz$`, `^drivers/.+\.csv\.gz$`, `^stints/.+\.csv\.gz$`, `^pit/.+\.csv\.gz$`, `^laps/.+\.csv\.gz$`.
3. **Snowflake**: create `F1_ANALYTICS_DEV` DB with `RAW`, `STAG`, `PROD`; grant roles/warehouse.
4. **dbt**: `dbt deps && dbt debug && dbt build --select tag:stg,int && dbt build --select tag:mart`.
5. **Power BI**: connect to Snowflake (PROD marts), build **Race Overview** & **Pit Strategy** pages.

**Ops Notes**
- To fetch **only new** GPs, store processed `meeting_key` (DynamoDB) or check if `day=` path exists before writing.
- If Fivetran “misses” files, re-validate role/base path and confirm regex match.
- Prefer `try_to_numeric` in Snowflake for safe casting; dedupe before `LISTAGG`.

> See the **Cookbook** for full step-by-step implementation details.
