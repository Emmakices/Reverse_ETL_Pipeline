# Reverse ETL Pipeline — Project Documentation

> **Author:** terrabog
> **Last updated:** 2026-02-18
> **Status:** Production-ready

---

## Table of Contents

1. [What Is This Project?](#1-what-is-this-project)
2. [How the Pipeline Works — The Big Picture](#2-how-the-pipeline-works--the-big-picture)
3. [Project Structure](#3-project-structure)
4. [Module Reference](#4-module-reference)
5. [Data Model & Schema](#5-data-model--schema)
6. [Configuration Reference](#6-configuration-reference)
7. [Running the Pipeline](#7-running-the-pipeline)
8. [Airflow Deployment](#8-airflow-deployment)
9. [Reliability Patterns](#9-reliability-patterns)
10. [Observability & Logging](#10-observability--logging)
11. [Error Handling](#11-error-handling)
12. [Testing](#12-testing)
13. [Troubleshooting](#13-troubleshooting)

---

## 1. What Is This Project?

This is a **Reverse ETL pipeline** — a weekly automated process that pulls raw e-commerce event data from a REST API, validates and transforms it, stores it in both local disk and Azure Data Lake Storage Gen2 (ADLS), runs quality gates, and finally pushes aggregated customer metrics to Salesforce.

The name "Reverse ETL" reflects the direction of data flow: instead of loading data *into* a data warehouse, this pipeline takes data *out of* internal systems and pushes it into an external CRM (Salesforce) where business teams can act on it.

### What problem does it solve?

Before this pipeline, someone would have to manually export customer activity data and upload it to Salesforce every week — a tedious, error-prone process. This pipeline fully automates that, runs on a schedule via Apache Airflow, retries gracefully on failure, tracks every run in a database for audit purposes, and handles bad data without crashing.

### Technology Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.9.3 |
| Language | Python 3.12 |
| Data Processing | pandas, PyArrow |
| API Client | requests + tenacity (retry) |
| Data Validation | Pydantic v2 |
| Cloud Storage | Azure Data Lake Storage Gen2 |
| Database | Azure SQL (pyodbc + AAD auth) |
| CRM Sync | Salesforce (simple-salesforce) |
| Containerisation | Docker + Docker Compose |
| Testing | pytest + responses (mocked HTTP) |

---

## 2. How the Pipeline Works — The Big Picture

Every time the pipeline runs, it processes data for a single week window (`week_start` to `week_end`). Here is the full journey data takes from source to destination:

```
┌─────────────────────────────────────────────────────────────────┐
│                        PIPELINE FLOW                            │
│                                                                 │
│  ┌──────────┐    ┌──────────────┐    ┌──────────┐              │
│  │ REST API │───▶│   EXTRACT    │───▶│ Raw JSON │              │
│  │(ecom     │    │ (paginated   │    │ (sidecar │              │
│  │ events)  │    │  + retries)  │    │  file)   │              │
│  └──────────┘    └──────────────┘    └────┬─────┘              │
│                                           │                     │
│                                           ▼                     │
│                                  ┌──────────────────┐           │
│                                  │ VALIDATE &       │           │
│                                  │ TRANSFORM        │           │
│                                  │ (Pydantic schema │           │
│                                  │  + DQ checks)    │           │
│                                  └───────┬──────────┘           │
│                                          │                      │
│                         ┌────────────────┴──────────────┐       │
│                         ▼                               ▼       │
│                  ┌─────────────┐                 ┌───────────┐  │
│                  │ Valid Data  │                 │  Rejects  │  │
│                  │  DataFrame  │                 │ DataFrame │  │
│                  └──────┬──────┘                 └─────┬─────┘  │
│                         │                             │         │
│                         ▼                             ▼         │
│                  ┌─────────────────────────────────────────┐    │
│                  │               LOAD                      │    │
│                  │  Local Parquet → ADLS Gen2 upload        │    │
│                  │  (events.parquet + rejects.parquet)      │    │
│                  └─────────────────────────────────────────┘    │
│                                    │                            │
│                                    ▼                            │
│                         ┌─────────────────────┐                 │
│                         │   QUALITY GATE      │                 │
│                         │ reject rate ≤ 5%?   │                 │
│                         └──────────┬──────────┘                 │
│                                    │ PASS                       │
│                                    ▼                            │
│                  ┌─────────────────────────────────────────┐    │
│                  │          SALESFORCE SYNC                │    │
│                  │  Azure SQL view → bulk upsert to CRM    │    │
│                  └─────────────────────────────────────────┘    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Stage-by-Stage Summary

| Stage | Task ID in Airflow | What it does |
|---|---|---|
| **Extract** | `extract` | Pages through the ecom events API, fetches all records for the week, saves raw JSON to a sidecar file |
| **Validate & Transform** | `validate_and_transform` | Validates every record against the `EventRecord` Pydantic schema, separates valid from invalid (rejects), normalises both into DataFrames |
| **Load** | `load` | Writes valid events + rejects to local Parquet files, uploads both to Azure Data Lake Storage Gen2 |
| **Quality Gate** | `quality_gate` | Checks if the reject rate is within the configured threshold (default 5%). Fails the pipeline if too many records were bad |
| **Salesforce Sync** | `salesforce_sync` | Reads aggregated customer metrics from an Azure SQL view, bulk-upserts them into a custom Salesforce object (`Customer_Weekly_Metric__c`) |

Each stage is **independent and resumable** — if a stage fails, Airflow retries just that stage without re-running the earlier ones. Stages communicate via small JSON sidecar files written to the output directory.

---

## 3. Project Structure

```
Reverse_ ETL/
│
├── extractor/                  # Core pipeline package
│   ├── __init__.py
│   ├── cli.py                  # CLI entry point — argparse + stage dispatch
│   ├── config.py               # Pydantic Settings — all configuration lives here
│   ├── models.py               # Data models: EventRecord, PipelineMetrics, etc.
│   ├── extraction.py           # REST API client — pagination, retry, rate limiting
│   ├── transformation.py       # Validation, normalisation, data quality checks
│   ├── storage.py              # Local Parquet writes + ADLS Gen2 upload
│   ├── checkpoint.py           # JSON state file + JSONL pending update queue
│   ├── azure_sql.py            # Azure SQL: checkpoint, run tracking, stage tracking
│   ├── pipeline.py             # Full pipeline + per-stage runner functions
│   ├── salesforce_sync.py      # Fetch from Azure SQL view → push to Salesforce
│   ├── logging_utils.py        # Structured JSON logging, correlation IDs
│   └── exceptions.py           # Custom exception hierarchy
│
├── tests/                      # Test suite (191 tests, 90% coverage)
│   ├── conftest.py             # Shared fixtures
│   ├── test_config.py
│   ├── test_models.py
│   ├── test_extraction.py
│   ├── test_transformation.py
│   ├── test_storage.py
│   ├── test_checkpoint.py
│   ├── test_azure_sql.py
│   ├── test_pipeline.py
│   ├── test_salesforce_sync.py
│   ├── test_logging_utils.py
│   ├── test_cli.py
│   └── test_integration.py     # Full end-to-end pipeline test (all external calls mocked)
│
├── airflow/
│   ├── dags/
│   │   └── reverse_etl_weekly.py   # Airflow DAG definition
│   ├── Dockerfile                  # Custom image: Airflow + ODBC Driver 18 + Python deps
│   ├── docker-compose.yaml         # Full stack: postgres, airflow, ecom-api
│   ├── requirements.txt            # Production-only Python deps for Docker image
│   ├── .env.example                # Template for environment variables
│   └── .env                        # Your local config (not committed)
│
├── docs/
│   ├── PROJECT_DOCUMENTATION.md    # This file
│   ├── PIPELINE_REPORT.md
│   └── DETAILED_PIPELINE_ANALYSIS.md
│
├── data/                       # Pipeline output (created at runtime)
│   └── raw_events/
│       └── week_start=YYYY-MM-DD/
│           └── week_end=YYYY-MM-DD/
│               ├── events.parquet
│               └── _stage_*.json   # Inter-stage metadata sidecars
│
├── state/                      # Checkpoint files (created at runtime)
│   ├── extractor_state.json
│   └── pending_db_updates.jsonl
│
├── extract_events.py           # Thin shim → delegates to extractor.cli.main()
├── requirements.txt            # Python dependencies
├── pyproject.toml              # Pytest config
└── config.env                  # Local configuration (not committed)
```

---

## 4. Module Reference

### `config.py` — Configuration

This is the single source of truth for all pipeline settings. It uses `pydantic-settings`, which means every setting can be provided via environment variables or a `config.env` file — no need to hardcode anything.

The config is loaded once at the start of every run. If a required field is missing or a value is invalid (e.g., `week_end` before `week_start`, a malformed URL, an invalid log level), the pipeline fails immediately with a clear error rather than discovering the problem halfway through a run.

Key properties computed from the config:

| Property | Example Value |
|---|---|
| `local_output_file` | `data/raw_events/week_start=2019-12-01/week_end=2019-12-07/events.parquet` |
| `adls_output_path` | `reverse_etl/raw_events/week_start=2019-12-01/week_end=2019-12-07/events.parquet` |
| `local_rejects_output_file` | `data/raw_events_rejects/week_start=.../rejects.parquet` |
| `state_file` | `state/extractor_state.json` |
| `pending_updates_file` | `state/pending_db_updates.jsonl` |

---

### `extraction.py` — API Client

Handles everything related to fetching data from the ecom events API.

**Pagination:** The API is paged (up to 1,000 records per page). The extractor loops through pages automatically, stopping when it receives an empty page, a partial page, or when the total count from the API metadata is reached.

**Retry logic:** Two layers of retry protection:
- `urllib3 Retry` on the HTTP adapter handles `429`, `500`, `502`, `503`, `504` status codes automatically
- `tenacity` adds application-level retry for `ConnectionError` (3 attempts, exponential backoff 2s → 30s)

**Rate limiting:** A configurable delay (`api_rate_limit_delay`, default 2 seconds) is applied between page fetches to avoid hammering the API.

**Flexible response format:** The API can return either a bare list or a dict with a `data`, `events`, `results`, or `items` key — the extractor handles all of these gracefully.

---

### `transformation.py` — Validation & Transformation

This module is responsible for turning raw API data into clean, trustworthy DataFrames.

**Step 1 — Schema Validation (`validate_events`):**
Every record is run through `EventRecord.model_validate()`. Records that pass are collected into `valid_events`; those that fail are captured in `rejected_events` with the full Pydantic error detail and the original raw data. This means you always know *why* a record was rejected.

**Step 2 — Normalisation (`normalize_events`):**
Valid events are converted to a DataFrame with a fixed schema. Three metadata columns are added:
- `batch_week_start` — the week start date
- `batch_week_end` — the week end date
- `ingested_at` — UTC timestamp of when this batch was processed

**Step 3 — Data Quality Checks (`run_data_quality_checks`):**
After normalisation, the valid DataFrame is checked for:
- Minimum record count (must have at least 1 row by default)
- Null `user_id` values (must be zero)
- Events outside the expected date range
- Negative prices

If any check fails, a `DataQualityError` is raised.

**Step 4 — Reject Normalisation (`normalize_rejects`):**
Rejected records are also normalised into a DataFrame and saved separately, so the data team can investigate bad data without losing it.

---

### `storage.py` — Local & Cloud Storage

Handles writing data to disk and uploading to Azure.

**Local writes:**
Data is written as Parquet (Snappy compression, PyArrow engine). After writing, the file's SHA-256 hash and byte size are computed and returned as metadata — useful for integrity verification and audit logging.

**ADLS Gen2 upload:**
Uses `DataLakeServiceClient` from the Azure SDK. Authentication follows the `DefaultAzureCredential` chain — it will use a service principal (via `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID` env vars) in production, or your local `az login` session for development.

The upload path mirrors the local path structure: `reverse_etl/raw_events/week_start=YYYY-MM-DD/week_end=YYYY-MM-DD/events.parquet`. This Hive-style partitioning makes the data directly queryable by tools like Azure Synapse or Databricks.

---

### `azure_sql.py` — Azure SQL Integration

This module manages all database interactions. The Azure SQL instance serves as the authoritative audit trail and checkpoint store.

**Authentication:**
Supports both SQL username/password auth (set `AZURE_SQL_USERNAME` + `AZURE_SQL_PASSWORD`) and AAD token auth via `DefaultAzureCredential` (used in production with service principal or Managed Identity). The ODBC access token is formatted as required by the driver: 4-byte little-endian length prefix + UTF-16LE encoded token.

**Tables used:**

| Table | Purpose |
|---|---|
| `dbo.pipeline_checkpoint` | Stores `next_week_start` / `next_week_end` to resume from the right week |
| `dbo.pipeline_run` | One row per pipeline execution — status, row counts, reject rate, timing |
| `dbo.pipeline_stage` | One row per stage per run — granular start/end times, row counts, errors |

**Views used:**

| View | Purpose |
|---|---|
| `dbo.vw_salesforce_customer_weekly_export` | Pre-aggregated customer metrics ready for Salesforce |

**Pending update replay (`flush_pending_db_updates`):**
If a DB write fails during a run, the update is saved to `pending_db_updates.jsonl`. On the next run, before anything else happens, the pipeline reads that file and attempts to replay each failed write. Successfully replayed updates are removed from the file; failed ones stay for the next attempt.

---

### `checkpoint.py` — Checkpoint Management

Manages the JSON state file (`state/extractor_state.json`), which is a mirror/backup of the checkpoint stored in Azure SQL.

The JSON file is never used as the *primary* checkpoint source — Azure SQL is. But the JSON file is always written after a successful run so there's a human-readable record of the last run's state locally. This is also useful for debugging and manual inspection.

The JSONL pending updates file (`state/pending_db_updates.jsonl`) acts as a durable queue. Each line is a self-contained JSON object representing one failed DB write, including the checkpoint and run payload. The atomic write pattern (write to `.tmp` then rename) ensures the file is never left in a corrupt state if the process crashes mid-write.

---

### `pipeline.py` — Pipeline Orchestration

This is the brain of the operation. It contains two execution modes:

**Full pipeline (`run_pipeline`):**
Runs all stages sequentially in a single function call. Used for direct invocation (e.g., `python -m extractor.cli` without `--stage`). All stage tracking against Azure SQL is handled here.

**Per-stage runners:**
Each stage has its own function (`run_extract_stage`, `run_transform_stage`, `run_load_stage`, `run_quality_gate_stage`) that reads from and writes to JSON sidecar files. This is what Airflow calls when using the multi-task DAG.

**Stage metadata sidecars:**
Stages pass data to each other via small JSON files named `_stage_{name}_{run_tag}.json` in the output directory. The `run_tag` is set from `AIRFLOW_CTX_DAG_RUN_ID`, which prevents metadata files from different concurrent DAG runs from colliding.

After a successful full run, all sidecar files and intermediate Parquet files are cleaned up automatically.

---

### `salesforce_sync.py` — Salesforce Integration

Handles the final step of the pipeline — pushing aggregated customer data to Salesforce.

**Data source:**
Rather than computing metrics in Python, this module queries the `dbo.vw_salesforce_customer_weekly_export` SQL view, which is pre-filtered for the current `week_start` / `week_end`. The DB does the heavy aggregation work; Python just transports the results.

**Salesforce upsert:**
Uses the Salesforce Bulk API for efficient batch processing (10,000 records per batch). The external ID field `External_Signal_Id__c` is used for upsert — if a record already exists in Salesforce for that signal ID, it's updated; otherwise a new record is inserted.

**Error tolerance:**
Up to 10% of records in a batch can fail without the sync aborting. If more than 10% fail, the entire sync is marked as failed and the pipeline errors out. The first 5 error samples are logged for investigation.

**Credentials:**
Sourced from environment variables `SF_USERNAME`, `SF_PASSWORD`, `SF_SECURITY_TOKEN`, and optionally `SF_DOMAIN` (defaults to `login`; use `test` for sandbox).

---

### `cli.py` — Command-Line Interface

The entry point for everything. Whether Airflow calls the pipeline, you run it locally, or a test invokes it directly — it all goes through `cli.py`.

**Key flags:**

| Flag | Purpose |
|---|---|
| `--week-start / --week-end` | Explicit date override (bypasses checkpoint) |
| `--use-checkpoint` | Read next week from Azure SQL checkpoint table |
| `--stage` | Run a single stage: `extract`, `transform`, `load`, `quality-gate`, `salesforce-sync`, `full` |
| `--local-only` | Skip ADLS upload (useful for local testing) |
| `--dry-run` | Validate only — don't write any files |
| `--force` | Re-run even if output file already exists |
| `--check-api` | Quick health check of the API endpoint, then exit |
| `--log-format` | `json` (default, for Airflow log parsing) or `text` (human-readable) |
| `--advance-on-skip` | Advance the checkpoint even if the run was skipped (output existed) |

**Checkpoint advance logic:**
After a successful run, the checkpoint is advanced to the *next* week automatically (e.g., `2019-12-01→07` becomes `2019-12-08→14`). The week is always 7 days (Mon–Sun). The checkpoint is only advanced on `SUCCESS` or `SUCCESS_WITH_REJECTS` — a `FAILED` run leaves the checkpoint unchanged so the same week is retried next time.

---

### `logging_utils.py` — Structured Logging

All log output is structured JSON (when `--log-format json`, which is the Airflow default). Every log line includes:

- `timestamp` — UTC ISO-8601
- `level` — DEBUG / INFO / WARNING / ERROR
- `logger` — module name (e.g., `extractor.extraction`)
- `message` — human-readable description
- `correlation_id` — UUID generated per pipeline run, present on every log line
- Any additional context fields passed via `extra={}`

This makes log lines directly queryable in Azure Monitor, Datadog, or any log aggregator.

The `log_operation` context manager wraps any block of code and automatically logs start, end, and duration — plus the full exception if one occurs. It's used throughout the codebase to provide consistent timing visibility.

---

### `exceptions.py` — Exception Hierarchy

```
ExtractorError (base)
├── ConfigurationError        — bad/missing config
├── APIError (base)
│   ├── APIConnectionError    — network unreachable
│   ├── APITimeoutError       — request timed out
│   └── APIResponseError      — unexpected status/payload
├── ValidationError (base)
│   ├── SchemaValidationError — overall schema pass rate too low
│   └── DataQualityError      — DQ checks failed (null IDs, out-of-range, negatives)
├── StorageError (base)
│   ├── LocalStorageError     — disk write failed
│   └── AzureStorageError     — ADLS upload failed
├── AuthenticationError       — Azure or Salesforce auth failed
└── CheckpointError           — DB checkpoint read/write failed
```

Every exception carries a `details` dict with machine-readable context (e.g., `{"url": ..., "status_code": 503}`) alongside the human-readable message.

---

## 5. Data Model & Schema

### Input — `EventRecord` (Pydantic)

This is what the API is expected to return per record. All fields are validated on ingestion.

| Field | Type | Required | Notes |
|---|---|---|---|
| `event_time` | `datetime` | Yes | ISO-8601 string or datetime object; coerced to UTC |
| `event_type` | `str` | Yes | Lowercased and stripped on validation |
| `user_id` | `str` | Yes | Cannot be empty; numeric IDs are coerced to string |
| `product_id` | `str \| None` | No | Numeric IDs coerced to string |
| `category_id` | `str \| None` | No | Numeric IDs coerced to string |
| `category_code` | `str \| None` | No | |
| `brand` | `str \| None` | No | |
| `price` | `Decimal \| None` | No | Must be ≥ 0; coerced from int/float/string |
| `user_session` | `str \| None` | No | |

Extra fields from the API are silently ignored (`extra="ignore"`). Whitespace is stripped from all string fields automatically.

### Output — Parquet Schema

The output Parquet file always has exactly these columns:

| Column | Type | Source |
|---|---|---|
| `event_time` | `datetime[UTC]` | From `EventRecord` |
| `event_type` | `str` | From `EventRecord` |
| `product_id` | `str \| null` | From `EventRecord` |
| `category_id` | `str \| null` | From `EventRecord` |
| `category_code` | `str \| null` | From `EventRecord` |
| `brand` | `str \| null` | From `EventRecord` |
| `price` | `float \| null` | From `EventRecord` |
| `user_id` | `str` | From `EventRecord` |
| `user_session` | `str \| null` | From `EventRecord` |
| `batch_week_start` | `str` | Added at normalisation (e.g., `"2019-12-01"`) |
| `batch_week_end` | `str` | Added at normalisation (e.g., `"2019-12-07"`) |
| `ingested_at` | `str` | Added at normalisation (UTC ISO-8601) |

### Rejects Schema

| Column | Type | Notes |
|---|---|---|
| `row_index` | `int` | Original position in the raw API response |
| `error_count` | `int` | Number of Pydantic validation errors on this record |
| `errors` | `list[dict]` | Full Pydantic error detail |
| `raw_event` | `dict` | Original unmodified record from the API |
| `rejected_at` | `str` | UTC timestamp of rejection |
| `batch_week_start` | `str` | Week context |
| `batch_week_end` | `str` | Week context |

### Run Status Values

| Status | Meaning |
|---|---|
| `SUCCESS` | All records valid, no rejects |
| `SUCCESS_WITH_REJECTS` | Some records rejected, but reject rate ≤ threshold |
| `FAILED` | Reject rate exceeded threshold, or a stage errored out |

---

## 6. Configuration Reference

Configuration is loaded from `config.env` in the working directory, or from environment variables (env vars take precedence).

### Required Settings

| Variable | Example | Description |
|---|---|---|
| `API_BASE_URL` | `http://ecom-api:8000` | Base URL of the ecom events API |
| `API_KEY` | `your-api-key` | API authentication key (`X-API-KEY` header) |
| `WEEK_START` | `2019-12-01` | Start date of the batch week (YYYY-MM-DD) |
| `WEEK_END` | `2019-12-07` | End date of the batch week (inclusive, YYYY-MM-DD) |

### Azure Storage (Optional — omit both to run local-only)

| Variable | Example | Description |
|---|---|---|
| `STORAGE_ACCOUNT` | `mystorageaccount` | Azure Storage account name |
| `CONTAINER` | `datalake` | ADLS container name |
| `ADLS_BASE_PATH` | `reverse_etl/raw_events` | Root path in ADLS for valid events |
| `ADLS_REJECTS_BASE_PATH` | `reverse_etl/raw_events_rejects` | Root path in ADLS for rejects |

> Both `STORAGE_ACCOUNT` and `CONTAINER` must be set together or not at all.

### Azure SQL (Required for DB checkpoint and run tracking)

| Variable | Default | Description |
|---|---|---|
| `AZURE_SQL_SERVER` | — | SQL Server hostname (e.g., `myserver.database.windows.net`) |
| `AZURE_SQL_DATABASE` | — | Database name |
| `AZURE_SQL_USERNAME` | — | SQL auth username (optional — omit to use AAD token auth) |
| `AZURE_SQL_PASSWORD` | — | SQL auth password (optional) |
| `AZURE_SQL_DRIVER` | `ODBC Driver 18 for SQL Server` | ODBC driver name |

### API Behaviour

| Variable | Default | Description |
|---|---|---|
| `API_TIMEOUT_SECONDS` | `60` | Per-request timeout in seconds (1–300) |
| `API_MAX_RETRIES` | `3` | Max HTTP retry attempts (1–10) |
| `API_PAGE_SIZE` | `1000` | Records per API page (max 1000) |
| `API_RATE_LIMIT_DELAY` | `2.0` | Seconds to wait between page fetches |

### Pipeline Behaviour

| Variable | Default | Description |
|---|---|---|
| `REJECT_THRESHOLD_PCT` | `5.0` | Max allowed reject percentage (0–100). Exceeding this fails the pipeline |
| `LOCAL_DATA_DIR` | `data/raw_events` | Local directory for output Parquet files |
| `LOCAL_REJECTS_DIR` | `data/raw_events_rejects` | Local directory for rejects Parquet files |
| `STATE_DIR` | `state` | Directory for checkpoint and pending update files |
| `LOG_LEVEL` | `INFO` | Logging verbosity (DEBUG, INFO, WARNING, ERROR, CRITICAL) |

### Salesforce (Runtime env vars, set in Airflow Variables)

| Variable | Description |
|---|---|
| `SF_USERNAME` | Salesforce username |
| `SF_PASSWORD` | Salesforce password |
| `SF_SECURITY_TOKEN` | Salesforce security token |
| `SF_DOMAIN` | `login` (production) or `test` (sandbox) |
| `SF_OBJECT_NAME` | Salesforce object API name (default: `Customer_Weekly_Metric__c`) |

---

## 7. Running the Pipeline

### Prerequisites

```bash
# Create and activate the virtual environment
python -m venv .venv
.venv/Scripts/activate   # Windows

# Install dependencies
pip install -r requirements.txt

# Copy and fill in config
cp config.env.example config.env   # edit with your values
```

### Common CLI Invocations

**Run the full pipeline for a specific week (local only, no ADLS):**
```bash
python -m extractor.cli \
  --week-start 2019-12-01 \
  --week-end 2019-12-07 \
  --local-only \
  --log-format text
```

**Dry run — validate data without writing files:**
```bash
python -m extractor.cli \
  --week-start 2019-12-01 \
  --week-end 2019-12-07 \
  --dry-run \
  --log-format text
```

**Run with automatic checkpoint (reads next week from DB):**
```bash
python -m extractor.cli --use-checkpoint --log-format text
```

**Force re-run even if output already exists:**
```bash
python -m extractor.cli \
  --week-start 2019-12-01 \
  --week-end 2019-12-07 \
  --force
```

**Run individual stages manually:**
```bash
python -m extractor.cli --week-start 2019-12-01 --week-end 2019-12-07 --stage extract
python -m extractor.cli --week-start 2019-12-01 --week-end 2019-12-07 --stage transform
python -m extractor.cli --week-start 2019-12-01 --week-end 2019-12-07 --stage load --local-only
python -m extractor.cli --week-start 2019-12-01 --week-end 2019-12-07 --stage quality-gate
python -m extractor.cli --week-start 2019-12-01 --week-end 2019-12-07 --stage salesforce-sync
```

**Check API health:**
```bash
python -m extractor.cli --check-api --log-format text
```

### Exit Codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Pipeline failed (extraction error, data quality failure, config issue, etc.) |
| `130` | Interrupted by user (Ctrl+C) |

---

## 8. Airflow Deployment

### Stack Setup

The full stack is defined in `airflow/docker-compose.yaml`. It includes:

| Service | Role |
|---|---|
| `postgres` | Airflow metadata database |
| `airflow-init` | One-shot service: runs DB migration + creates admin user |
| `airflow-webserver` | Hosts the Airflow UI at `localhost:8080` |
| `airflow-scheduler` | Triggers DAG runs on schedule |
| `ecom-api` | The e-commerce events API (local image: `ecom-api:1.0`) |

**Starting the stack:**
```bash
cd airflow
docker compose up -d
```

**First-time setup — copy and edit `.env`:**
```bash
cp .env.example .env
# Edit .env — fill in passwords, API keys, and the PROJECT_ROOT path
```

The `PROJECT_ROOT` variable must be the absolute path to the repo root (e.g., `C:/Users/User/Desktop/Reverse_ ETL`). This is mounted into the containers at `/opt/project` so Airflow tasks can run `python -m extractor.cli`.

### Airflow Variables

Set these in the Airflow UI under **Admin → Variables**:

| Variable | Value | Notes |
|---|---|---|
| `pipeline_mode` | `local` or `azure` | `local` = explicit dates, no ADLS/SF. `azure` = full stack with DB checkpoint |
| `week_start` | `2019-12-01` | Used in local mode only |
| `week_end` | `2019-12-07` | Used in local mode only |
| `sf_username` | your SF username | Required for Salesforce sync |
| `sf_password` | your SF password | Required for Salesforce sync |
| `sf_security_token` | your SF token | Required for Salesforce sync |
| `sf_domain` | `login` or `test` | Optional, defaults to `login` |
| `slack_webhook_url` | your Slack webhook URL | Optional — enables failure alerts |

### DAG Overview

**DAG ID:** `reverse_etl_customer_weekly`
**Schedule:** Mon–Fri at 14:00 UTC (`0 14 * * 1-5`)
**Max concurrent runs:** 1 (prevents overlapping extractions)
**Catchup:** Disabled (won't backfill missed runs)

**Task defaults:**
- 3 retries per task
- Exponential backoff: 5 min → max 30 min between retries
- Per-task execution timeout: 2h for extract, 30min for transform/load/SF sync, 5min for quality gate
- Entire DAG run timeout: 4 hours

**Task dependency:**
```
extract → validate_and_transform → load → quality_gate → salesforce_sync
```

Salesforce sync is only executed if `pipeline_mode=azure` **and** `sf_username` is set in Airflow Variables. Otherwise it echoes a skip message and exits cleanly.

### Triggering the DAG

**Via Airflow UI:**
1. Open `http://localhost:8080`
2. Log in (default: `admin` / your `.env` password)
3. Find `reverse_etl_customer_weekly`
4. Unpause the DAG (toggle switch)
5. Click **▶ Trigger DAG** (top-right)
6. Optionally pass a JSON config to override settings

**Via CLI:**
```bash
docker exec airflow-webserver airflow dags trigger reverse_etl_customer_weekly
```

**With a config override:**
```bash
docker exec airflow-webserver airflow dags trigger reverse_etl_customer_weekly \
  --conf '{"week_start": "2019-12-01", "week_end": "2019-12-07"}'
```

### Verifying Each Stage

After triggering, watch the task states in the **Graph View** of the DAG run. Each task turns green when it succeeds:

```
extract ✅ → validate_and_transform ✅ → load ✅ → quality_gate ✅ → salesforce_sync ✅
```

Click any task → **Log** to see its full structured output. Look for these confirmation messages:

| Stage | What to look for in logs |
|---|---|
| extract | `"Fetched all events from API"` with `total_rows: N` |
| validate_and_transform | `"Schema validation complete"` with `valid_records / total_records` |
| load | `"File uploaded to ADLS successfully"` with `path` and `size_bytes` |
| quality_gate | `"Quality gate PASSED"` |
| salesforce_sync | `"Salesforce sync complete"` with `success_count: N` |

---

## 9. Reliability Patterns

### Idempotency

Every pipeline run is safe to re-run for the same week. Before doing any work, the pipeline checks if the output Parquet file already exists locally. If it does, the run is skipped (output counts as done). Use `--force` to override this and re-extract anyway.

If the output exists locally but the ADLS upload failed previously, the pipeline re-attempts the cloud upload on the next skip-detected run.

### DB-First Checkpoint

The checkpoint (which week to run next) is stored in Azure SQL — not the local JSON file. The JSON file is a human-readable mirror only.

On startup with `--use-checkpoint`:
1. Connect to Azure SQL
2. Read `next_week_start` / `next_week_end` from `dbo.pipeline_checkpoint`
3. If no row exists, fall back to `config.env` defaults
4. If the checkpoint is stale (not updated in 14+ days), log a warning

After a successful run, the checkpoint is advanced to the next week (e.g., `2019-12-01` → `2019-12-08`). The checkpoint is never advanced on a failed run.

### Pending Update Replay

If the DB checkpoint write fails (network blip, transient SQL error), the update isn't lost — it's appended to `state/pending_db_updates.jsonl`. On the very next run (before reading the checkpoint), the pipeline reads that file and replays each pending update. Successfully replayed entries are removed; failed ones stay for the next attempt.

This means the checkpoint will eventually be consistent even if the DB is briefly unreachable.

### Per-Stage Observability

Every pipeline run creates a row in `dbo.pipeline_run` and a row per stage in `dbo.pipeline_stage`. You can query these tables to:

- See which week each run processed
- Compare rows fetched vs rows output vs rows rejected
- Find which specific stage failed and why
- Track the reject rate over time

### Failure Alerting

If any task fails in the DAG, `on_task_failure` is called automatically. It logs the failure details and — if `slack_webhook_url` is set in Airflow Variables — sends a Slack notification with the task name, DAG ID, execution date, exception message, and a direct link to the task log.

---

## 10. Observability & Logging

### Structured JSON Logs

Every log line is a JSON object when running in `json` format (the default in Airflow). This makes logs easy to parse, filter, and ship to a log aggregator. Example:

```json
{
  "timestamp": "2026-02-18T14:05:32.001Z",
  "level": "INFO",
  "logger": "extractor.extraction",
  "message": "Fetched page 1",
  "correlation_id": "a3f8c2d1-...",
  "url": "http://ecom-api:8000/datasets/ecom_events",
  "records_in_page": 1000,
  "total_so_far": 1000
}
```

### Correlation ID

A UUID is generated at the start of each pipeline run and attached to every log line for that run via the `CorrelationIdFilter`. This means you can grep logs for a single correlation ID and see the complete story of one run across all modules.

### Operation Timing

The `log_operation` context manager wraps key operations and automatically logs:
- Start: `"Starting {operation}"` with context fields
- Success: `"Completed {operation}"` with `duration_ms`
- Failure: `"Failed {operation}"` with `duration_ms` and full exception traceback

### Database Metrics

After every run, `dbo.pipeline_run` is updated with:
- `rows_fetched`, `rows_output`, `rows_rejected`
- `reject_rate_pct`, `reject_threshold_pct`
- `pipeline_start_time`, `pipeline_end_time`
- `output_path`, `rejects_path`
- `status` (`SUCCESS`, `SUCCESS_WITH_REJECTS`, `FAILED`)
- `error_type`, `error_message` (if applicable)

---

## 11. Error Handling

The pipeline distinguishes between errors that should stop execution immediately and errors that should be tolerated:

**Hard failures (pipeline aborts):**
- `ConfigurationError` — bad config detected at startup
- `CheckpointError` — cannot read the DB checkpoint (can't know which week to run)
- `APIError` subtypes — all API errors after retries are exhausted
- `DataQualityError` — reject threshold exceeded or DQ checks failed

**Best-effort (log and continue):**
- DB stage tracking writes (`db_stage_start`, `db_stage_end`) — a failure here is logged but never stops the pipeline
- DB run finalization (`db_finalize_pipeline_run`) — logged, run continues
- DB checkpoint update — logged, queued to JSONL for replay

**Soft failures (never logged as errors):**
- Stage metadata cleanup after a successful run — any cleanup error is logged at DEBUG level only

This design means the pipeline never crashes silently due to an audit trail write failing. It always completes the work, then best-effort records the outcome.

---

## 12. Testing

### Running the Test Suite

```bash
# Run all tests
.venv/Scripts/python.exe -m pytest tests/ -v

# Run with coverage report
.venv/Scripts/python.exe -m pytest tests/ --cov=extractor --cov-report=term-missing

# Run only integration tests
.venv/Scripts/python.exe -m pytest tests/ -m integration -v

# Run a specific test file
.venv/Scripts/python.exe -m pytest tests/test_transformation.py -v
```

### Test Suite Overview

| Test File | Coverage area |
|---|---|
| `test_config.py` | Env var loading, validation, property computation |
| `test_models.py` | `EventRecord` field coercion, validators, `PipelineMetrics.to_dict()` |
| `test_extraction.py` | Pagination, retry, response format handling, rate limiting |
| `test_transformation.py` | Schema validation, normalisation, DQ checks, reject handling |
| `test_storage.py` | Parquet write, SHA256, ADLS upload (Azure SDK mocked) |
| `test_checkpoint.py` | JSON state read/write, JSONL pending updates, atomic rename |
| `test_azure_sql.py` | DB MERGE, checkpoint read/write, run tracking, flush pending |
| `test_pipeline.py` | Full pipeline, stage runners, skip-if-exists, reject threshold |
| `test_salesforce_sync.py` | SQL query, field mapping, bulk upsert, error rate check |
| `test_logging_utils.py` | JSON formatter, correlation ID filter, `log_operation` timing |
| `test_cli.py` | Arg parsing, stage dispatch, checkpoint advance logic |
| `test_integration.py` | End-to-end pipeline with fake HTTP and real file I/O |

**No real external services are needed to run the tests.** Azure SDK calls, SQL connections, HTTP requests, and Salesforce API calls are all mocked using `unittest.mock` and the `responses` library.

### Key Testing Patterns

- `responses` library intercepts HTTP requests and returns pre-defined API responses
- `unittest.mock.patch` is used for Azure SDK, pyodbc, and simple-salesforce calls
- Temporary directories (via `tmp_path` pytest fixture) are used for all file I/O so tests never touch real disk paths
- The `integration` marker separates end-to-end tests from unit tests

---

## 13. Troubleshooting

### Pipeline Issues

**`ConfigurationError: Failed to load configuration`**
Check that `config.env` exists in the working directory and all required variables (`API_BASE_URL`, `API_KEY`, `WEEK_START`, `WEEK_END`) are set.

**`APIConnectionError: Failed to connect to API`**
- Verify the API container is running: `docker compose ps ecom-api`
- Check the `API_BASE_URL` points to the right host (use `ecom-api` as hostname inside Docker, `localhost:8000` outside)
- Run `python -m extractor.cli --check-api` to test connectivity

**`APIResponseError: API response missing expected data key`**
The API returned a dict but none of the expected keys (`data`, `events`, `results`, `items`) were present. Check the API's response format has not changed.

**`CheckpointError: Failed to read checkpoint from database`**
Azure SQL is unreachable or credentials are wrong. Check:
- `AZURE_SQL_SERVER`, `AZURE_SQL_DATABASE` are correct
- The SQL firewall allows connections from your IP
- `az login` is active (for AAD auth), or SQL username/password are set

**`DataQualityError: Reject threshold exceeded`**
More than `REJECT_THRESHOLD_PCT`% of records failed validation. Check the rejects Parquet file at `data/raw_events_rejects/week_start=.../rejects.parquet` to understand why records are being rejected.

**`AuthenticationError: Azure auth failed`**
- For local development: run `az login`
- For production/Docker: ensure `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID` are set, and the service principal has the `Storage Blob Data Contributor` role on the storage account

### Airflow Issues

**DAG not appearing in the UI**
- Check scheduler logs: `docker compose logs airflow-scheduler`
- Verify the DAG file has no import errors: `docker exec airflow-scheduler python /opt/airflow/dags/reverse_etl_weekly.py`

**Task fails with `ModuleNotFoundError`**
The project code is not on the Python path inside the container. Verify `PROJECT_ROOT` in `.env` is correct and the volume mount is active: `docker exec airflow-webserver ls /opt/project/extractor`

**`load` task fails with ADLS permission error**
The Azure identity used inside the container doesn't have the right ADLS role. Assign `Storage Blob Data Contributor` to the service principal or Managed Identity on the storage account.

**Salesforce sync skipped even in azure mode**
Check that `sf_username` is set in Airflow Variables (**Admin → Variables**). The sync is gated on `bool(_sf_user)` being truthy.

**Checkpoint is stale (logged warning)**
The checkpoint row in `dbo.pipeline_checkpoint` hasn't been updated in 14+ days. This usually means the pipeline has been failing (and not advancing the checkpoint) or hasn't run at all. Check `dbo.pipeline_run` for recent failed runs to understand why.

### Checking Output Files

```bash
# List local output files
ls data/raw_events/

# Check what week the checkpoint is pointing to
cat state/extractor_state.json

# Inspect pending DB updates
cat state/pending_db_updates.jsonl

# Preview the Parquet file (requires Python + pandas)
python -c "
import pandas as pd
df = pd.read_parquet('data/raw_events/week_start=2019-12-01/week_end=2019-12-07/events.parquet')
print(df.shape)
print(df.dtypes)
print(df.head())
"
```

---

*This documentation was written to give any engineer — whether they built this pipeline or are encountering it for the first time — a clear, honest understanding of how every part works and why it was built that way. If something is unclear or out of date, update this file.*
