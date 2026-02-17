# Reverse ETL Pipeline — Analysis & Report

## 1. Architecture Overview

### Pipeline Flow
```
API Source --> Extract --> Validate & Transform --> Load (Parquet + ADLS) --> Quality Gate --> Salesforce Sync
```

### Components
| Component | File | Purpose |
|-----------|------|---------|
| CLI Entry | `extractor/cli.py` | Argument parsing, checkpoint orchestration, stage dispatch |
| Config | `extractor/config.py` | Pydantic-based config from `config.env`, field validation |
| Extraction | `extractor/extraction.py` | Paginated API fetch with retries, rate limiting |
| Transformation | `extractor/transformation.py` | Pydantic validation, pandas normalization, reject separation |
| Storage | `extractor/storage.py` | Parquet I/O, ADLS upload (valid + rejects), SHA256 integrity |
| Pipeline | `extractor/pipeline.py` | Full pipeline orchestration, stage tracking, quality gate |
| Checkpoint | `extractor/checkpoint.py` | JSON state read/write, pending DB update queue (JSONL) |
| Azure SQL | `extractor/azure_sql.py` | AAD-auth DB connection, checkpoint CRUD, run/stage tracking |
| Salesforce | `extractor/salesforce_sync.py` | Bulk upsert to Salesforce custom object via simple-salesforce |
| Logging | `extractor/logging_utils.py` | Structured JSON logging, correlation ID, timed operations |
| Models | `extractor/models.py` | EventRecord (Pydantic), PipelineMetrics, ValidationResult |
| Exceptions | `extractor/exceptions.py` | Typed exception hierarchy with detail payloads |

### Orchestration (Airflow)
- **DAG**: `airflow/dags/reverse_etl_weekly.py` — 5 tasks running weekly
- **Task chain**: `extract >> validate_and_transform >> load >> quality_gate >> salesforce_sync`
- **Docker**: `airflow/docker-compose.yaml` — PostgreSQL backend, health checks, env-based credentials

---

## 2. Key Design Patterns

### DB-First Checkpoint
- Azure SQL `dbo.pipeline_checkpoint` is the **source of truth** for the next batch week
- JSON file (`state/extractor_state.json`) is a **mirror/backup** only
- On startup, CLI reads checkpoint from DB via `--use-checkpoint` flag
- On success, checkpoint advances to next week (week_end + 1 day)

### Best-Effort DB Writes with Pending Replay
- If a DB checkpoint write fails post-extraction, the update is saved to `pending_db_updates.jsonl`
- On the **next run**, `flush_pending_db_updates()` auto-replays any pending updates
- Partial replay is supported: only successfully replayed items are removed from the pending file
- Uses atomic write (write-to-temp + rename) to prevent data loss

### Quality Gate
- Reject rate = `rows_rejected / rows_fetched * 100`
- Configurable threshold via `REJECT_THRESHOLD_PCT` (default 5%)
- If reject rate exceeds threshold: pipeline writes files but raises `DataQualityError`
- Run status: `SUCCESS` (0 rejects), `SUCCESS_WITH_REJECTS` (under threshold), `FAILED` (over threshold)

### Inter-Stage Data Passing
- Stage metadata stored as JSON sidecar files: `_stage_{name}_{run_tag}.json`
- Raw events saved as `_raw_events.json` between extract and transform
- Validated data saved as `_validated_events.parquet` between transform and load
- Run tag (from Airflow `run_id` or date) prevents collision across concurrent runs
- Stage metadata cleaned up after successful pipeline completion

---

## 3. Audit Findings & Fixes Applied

### Critical Issues (Fixed)
| # | Issue | Fix |
|---|-------|-----|
| 1 | API response silently fell back to empty list on unknown key | Now raises `APIResponseError` listing expected keys |
| 2 | Airflow DAG used Jinja `{{ var.value.xxx }}` in Python (not templates) | Replaced with `Variable.get()` calls |
| 3 | Docker Compose hardcoded `airflow:airflow` credentials | Replaced with `${POSTGRES_USER:-airflow}` env vars |

### High-Severity Issues (Fixed)
| # | Issue | Fix |
|---|-------|-----|
| 4 | No duplicate run prevention in SQL | Added filtered unique index on `(pipeline_name, batch_week_start, batch_week_end)` |
| 5 | Salesforce sync had no error rate threshold | Added 10% error rate threshold — raises `ExtractorError` above it |
| 6 | No stale checkpoint detection | CLI now warns if checkpoint hasn't been updated in 14+ days |
| 7 | ADLS upload not re-attempted on skip-if-exists | Now re-attempts ADLS upload when local file exists but cloud copy may be missing |
| 8 | Stage metadata files left behind after runs | Added `cleanup_stage_metadata()` after successful pipeline completion |

### Medium-Severity Issues (Fixed)
| # | Issue | Fix |
|---|-------|-----|
| 9 | Airflow `email_on_failure` enabled but no SMTP configured | Disabled email, relying on Slack webhook callback |
| 10 | No exponential backoff on retries | Added `retry_exponential_backoff: True` with `max_retry_delay: 30min` |
| 11 | Low retry count (2) | Increased to 3 retries |
| 12 | No DAG run timeout | Added `dagrun_timeout=timedelta(hours=4)` |
| 13 | `config.env.example` had typo `htp://` | Fixed to `http://` |
| 14 | SQL view had syntax error (`go CREATE`) | Fixed to `GO\nCREATE OR ALTER VIEW` |

---

## 4. Test Coverage Report

### Summary
- **Total tests**: 191
- **Overall coverage**: 90%
- **All tests pass**: Yes
- **External dependencies mocked**: Azure SQL, ADLS, Salesforce API, HTTP API

### Coverage by Module

| Module | Statements | Missed | Coverage |
|--------|-----------|--------|----------|
| `__init__.py` | 6 | 0 | 100% |
| `azure_sql.py` | 128 | 13 | 90% |
| `checkpoint.py` | 60 | 8 | 87% |
| `cli.py` | 139 | 16 | 88% |
| `config.py` | 88 | 5 | 94% |
| `exceptions.py` | 36 | 0 | 100% |
| `extract_events.py` | 4 | 4 | 0%* |
| `extraction.py` | 93 | 9 | 90% |
| `logging_utils.py` | 69 | 0 | 100% |
| `models.py` | 103 | 6 | 94% |
| `pipeline.py` | 280 | 57 | 80% |
| `salesforce_sync.py` | 54 | 0 | 100% |
| `storage.py` | 98 | 5 | 95% |
| `transformation.py` | 92 | 8 | 91% |
| **TOTAL** | **1250** | **131** | **90%** |

*`extract_events.py` is a 4-line shim (`if __name__ == "__main__": sys.exit(main())`) — not testable via import.

### Test Files
| Test File | Tests | What's Covered |
|-----------|-------|----------------|
| `test_azure_sql.py` | 21 | Connection, checkpoint CRUD, run/stage tracking, pending replay |
| `test_checkpoint.py` | 14 | JSON state R/W, pending updates (append, read, rewrite, malformed JSONL) |
| `test_cli.py` | 29 | Arg parsing, return codes, stage dispatch, checkpoint update, health check |
| `test_config.py` | 9 | Pydantic validation, computed properties, error cases |
| `test_extraction.py` | 8 | API fetch, pagination, retries, health check |
| `test_integration.py` | 7 | Full pipeline E2E with fake HTTP server (responses library) |
| `test_logging_utils.py` | 15 | JSON formatter, correlation ID, setup_logging, log_operation |
| `test_models.py` | 16 | EventRecord validation, PipelineMetrics, ValidationResult |
| `test_pipeline.py` | 21 | run_pipeline, stage metadata, cleanup, reject threshold, DB failure handling |
| `test_salesforce_sync.py` | 11 | SF config, fetch data, bulk upsert, error thresholds |
| `test_storage.py` | 23 | Parquet I/O, SHA256, ADLS upload/download, save_events, error handling |
| `test_transformation.py` | 13 | Validation, normalization, quality checks, full transform |

### Remaining Uncovered Lines (10%)
The uncovered lines are primarily:
- **`pipeline.py` (57 lines)**: Deep error-handling branches in DB stage tracking (try/except within try/except), ADLS re-upload on skip path, transform phase exception handlers
- **`cli.py` (16 lines)**: State path override, `KeyboardInterrupt` handler, no-checkpoint-found log branch
- **`azure_sql.py` (13 lines)**: `_get_sql_access_token_bytes` (AAD token encoding), `db_stage_end` and `db_finalize_pipeline_run` error paths
- **`extraction.py` (9 lines)**: Retry backoff logic, rate limit delay, max retries exceeded
- **`checkpoint.py` (8 lines)**: `write_json_state` exception handler, `rewrite_pending_db_updates` temp file cleanup on error

These are mostly defensive error-handling paths and cloud authentication code that's difficult to unit test without real infrastructure.

---

## 5. Security Review

### Secrets Management
- API key loaded from `config.env` (gitignored)
- Azure SQL uses AAD token auth (no password in code)
- Salesforce credentials from environment variables (`SF_USERNAME`, `SF_PASSWORD`, `SF_SECURITY_TOKEN`)
- Docker Compose credentials externalized to env vars
- `.gitignore` covers `.env`, `config.env`, `*.json` state files, output data

### Input Validation
- Pydantic `EventRecord` validates all API response fields
- `api_base_url` must start with `http://` or `https://`
- SQL queries use parameterized statements (no SQL injection risk)
- File paths use `pathlib.Path` (no shell injection risk)

### Error Handling
- Typed exception hierarchy (`ExtractorError` → `ConfigurationError`, `AuthenticationError`, etc.)
- All exceptions carry `details` dict for structured logging
- No sensitive data in error messages or logs

---

## 6. Deployment Checklist

### Prerequisites
- [ ] Azure SQL database provisioned with tables from `SQL_Script/config_tables.sql`
- [ ] Azure Data Lake Storage Gen2 account + container created
- [ ] Salesforce custom object `Customer_Weekly_Metric__c` configured
- [ ] `az login` or service principal configured for AAD auth
- [ ] ODBC Driver 18 for SQL Server installed

### Configuration
- [ ] `config.env` populated (copy from `config.env.example`)
- [ ] Salesforce env vars set: `SF_USERNAME`, `SF_PASSWORD`, `SF_SECURITY_TOKEN`
- [ ] Airflow variables set: `slack_webhook_url` (for failure notifications)

### Airflow Deployment
- [ ] Build Docker image: `cd airflow && docker-compose build`
- [ ] Initialize: `docker-compose up airflow-init`
- [ ] Start services: `docker-compose up -d`
- [ ] Verify DAG appears in Airflow UI
- [ ] Trigger manual run to validate end-to-end

### Monitoring
- Pipeline runs tracked in `dbo.pipeline_run` (Azure SQL)
- Stage-level tracking in `dbo.pipeline_stage`
- Checkpoint state in `dbo.pipeline_checkpoint`
- Structured JSON logs with correlation IDs
- Slack notifications on failure via `on_failure_callback`
