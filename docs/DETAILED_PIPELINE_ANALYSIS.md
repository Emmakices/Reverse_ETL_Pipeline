# Reverse ETL Pipeline — Detailed Educational Analysis

> This document provides an in-depth, educational walkthrough of every component, design
> decision, pattern, test strategy, audit finding, and production concern in this
> Reverse ETL pipeline. It is written so that someone new to the codebase (or to data
> engineering in general) can understand **what** the system does, **why** it is built
> this way, and **how** every piece fits together.

---

## Table of Contents

1. [What Is Reverse ETL?](#1-what-is-reverse-etl)
2. [Project Structure & File Map](#2-project-structure--file-map)
3. [Data Flow — End to End](#3-data-flow--end-to-end)
4. [Module-by-Module Deep Dive](#4-module-by-module-deep-dive)
   - 4.1 [exceptions.py — Error Hierarchy](#41-exceptionspy--error-hierarchy)
   - 4.2 [models.py — Data Models](#42-modelspy--data-models)
   - 4.3 [config.py — Configuration Management](#43-configpy--configuration-management)
   - 4.4 [logging_utils.py — Structured Logging](#44-logging_utilspy--structured-logging)
   - 4.5 [extraction.py — API Data Fetch](#45-extractionpy--api-data-fetch)
   - 4.6 [transformation.py — Validation & Normalization](#46-transformationpy--validation--normalization)
   - 4.7 [storage.py — Local & Cloud Storage](#47-storagepy--local--cloud-storage)
   - 4.8 [checkpoint.py — State Management](#48-checkpointpy--state-management)
   - 4.9 [azure_sql.py — Database Integration](#49-azure_sqlpy--database-integration)
   - 4.10 [pipeline.py — Orchestration Engine](#410-pipelinepy--orchestration-engine)
   - 4.11 [cli.py — Command-Line Interface](#411-clipy--command-line-interface)
   - 4.12 [salesforce_sync.py — CRM Integration](#412-salesforce_syncpy--crm-integration)
5. [Design Patterns Explained](#5-design-patterns-explained)
6. [Airflow Orchestration](#6-airflow-orchestration)
7. [Test Strategy & Coverage Analysis](#7-test-strategy--coverage-analysis)
8. [Audit Findings — Detailed Breakdown](#8-audit-findings--detailed-breakdown)
9. [Security Analysis](#9-security-analysis)
10. [Production Readiness Checklist](#10-production-readiness-checklist)

---

## 1. What Is Reverse ETL?

**Traditional ETL** (Extract, Transform, Load) moves data **from** operational systems
**into** a data warehouse for analytics.

**Reverse ETL** does the opposite: it moves **processed/aggregated data** from the
data warehouse **back into** operational systems like CRMs (Salesforce), marketing
platforms, or customer support tools.

### This Pipeline's Purpose

```
E-Commerce API  -->  Azure Data Lake (ADLS)  -->  Azure SQL (Synapse)  -->  Salesforce
     (raw events)     (parquet files)              (aggregated views)       (CRM records)
```

**Concrete example**: Every week, the pipeline:
1. Fetches raw e-commerce events (purchases, views, cart additions) from an API
2. Validates and transforms them into clean parquet files
3. Stores them in Azure Data Lake Storage Gen2
4. A Synapse view aggregates them into customer-level weekly metrics
5. Those metrics are pushed to Salesforce so sales reps can see customer behavior

---

## 2. Project Structure & File Map

```
Reverse_ ETL/
├── extractor/                    # Main Python package
│   ├── __init__.py              # Package init
│   ├── exceptions.py            # Custom exception hierarchy
│   ├── models.py                # Pydantic models & dataclasses
│   ├── config.py                # Configuration (Pydantic BaseSettings)
│   ├── logging_utils.py         # Structured JSON logging
│   ├── extraction.py            # API data fetching (pagination, retry)
│   ├── transformation.py        # Validation, normalization, quality checks
│   ├── storage.py               # Parquet I/O, ADLS upload
│   ├── checkpoint.py            # JSON state files, pending DB updates
│   ├── azure_sql.py             # Azure SQL connection, checkpoint DB ops
│   ├── pipeline.py              # Pipeline orchestration (full + stage runners)
│   ├── cli.py                   # CLI argument parsing, entry point
│   ├── salesforce_sync.py       # Salesforce bulk upsert
│   └── extract_events.py        # Legacy entry point (thin shim)
├── tests/                        # Test suite (191 tests)
│   ├── conftest.py              # Shared fixtures
│   ├── test_azure_sql.py        # 21 tests
│   ├── test_checkpoint.py       # 14 tests
│   ├── test_cli.py              # 29 tests
│   ├── test_config.py           # 9 tests
│   ├── test_extraction.py       # 8 tests
│   ├── test_integration.py      # 7 integration tests
│   ├── test_logging_utils.py    # 15 tests
│   ├── test_models.py           # 16 tests
│   ├── test_pipeline.py         # 21 tests
│   ├── test_salesforce_sync.py  # 11 tests
│   ├── test_storage.py          # 23 tests
│   └── test_transformation.py   # 13 tests
├── airflow/                      # Airflow deployment
│   ├── dags/
│   │   └── reverse_etl_weekly.py  # DAG definition (5 tasks)
│   └── docker-compose.yaml      # Airflow + PostgreSQL
├── SQL_Script/                   # Database schemas
│   ├── config_tables.sql        # pipeline_checkpoint, pipeline_run, pipeline_stage
│   └── synapse_script/
│       └── vw-customer-weekly_signals.sql  # Aggregation view for Salesforce
├── config.env.example           # Template for environment config
├── requirements.txt             # Python dependencies
└── pyproject.toml               # Pytest configuration
```

---

## 3. Data Flow — End to End

### Step-by-Step Flow

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   EXTRACT    │───>│  TRANSFORM   │───>│     LOAD     │───>│ QUALITY GATE │───>│  SALESFORCE  │
│              │    │              │    │              │    │              │    │    SYNC       │
│ Fetch events │    │ Pydantic     │    │ Write parquet│    │ Check reject │    │ Bulk upsert  │
│ from API     │    │ validation   │    │ Upload ADLS  │    │ rate vs      │    │ to SF custom │
│ (paginated)  │    │ + normalize  │    │              │    │ threshold    │    │ object       │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
       │                   │                   │                   │                   │
       v                   v                   v                   v                   v
  raw events          valid events +       events.parquet      PASS/FAIL          SF records
  (JSON list)         rejects (DataFrames) + rejects.parquet   decision           updated
```

### Data Transformations

| Stage | Input | Output | Key Operation |
|-------|-------|--------|---------------|
| Extract | API URL + date range | `list[dict]` (raw events) | Paginated HTTP GET with retry |
| Transform | `list[dict]` | `DataFrame` (valid) + `DataFrame` (rejects) | Pydantic validation, type coercion, lowercasing |
| Load | Two DataFrames | `.parquet` files on disk + ADLS | `df.to_parquet()` + Azure ADLS upload |
| Quality Gate | Transform metadata | Pass/Fail decision | `reject_rate > threshold` check |
| Salesforce | SQL view query | SF Bulk API upsert | DB column → SF field mapping |

---

## 4. Module-by-Module Deep Dive

### 4.1 exceptions.py — Error Hierarchy

**Purpose**: Provide typed, structured exceptions so callers can catch specific
failure types and react accordingly.

```
ExtractorError (base)
├── ConfigurationError          # Missing/invalid config
├── APIError
│   ├── APIConnectionError      # Network unreachable
│   ├── APITimeoutError         # Request timed out
│   └── APIResponseError        # Bad status code, invalid JSON, missing keys
├── ValidationError
│   ├── SchemaValidationError   # Too many invalid records (success rate < 95%)
│   └── DataQualityError        # Quality checks failed (null IDs, date range, etc.)
├── StorageError
│   ├── LocalStorageError       # Disk write failure
│   └── AzureStorageError       # ADLS upload failure
├── AuthenticationError         # Azure AD / credential failure
└── CheckpointError             # DB checkpoint read failure
```

**Why this matters**: Each exception type carries a `details` dict for structured
logging. This allows Airflow operators to distinguish between "API is down" (retry
later) vs "data quality failed" (needs investigation) vs "credentials expired"
(needs ops attention).

**Educational note — Exception Design Best Practices**:
- Every exception inherits from one base (`ExtractorError`) so you can catch all
  pipeline errors with a single handler
- The `details` dict avoids string-formatting sensitive data into error messages
- The hierarchy follows the "catch what you can handle" principle: `cli.py` catches
  `ConfigurationError` (return code 1), `CheckpointError` (return code 1), and
  `ExtractorError` (return code 1) at different specificity levels

---

### 4.2 models.py — Data Models

**Purpose**: Define the data contracts that events must conform to, and collect
runtime metrics.

#### EventRecord (Pydantic BaseModel)

```python
class EventRecord(BaseModel):
    event_time: datetime       # Required, ISO-8601 string → datetime
    event_type: str            # Required, auto-lowercased ("PURCHASE" → "purchase")
    user_id: str               # Required, coerced from int ("12345" or 12345 → "12345")
    product_id: str | None     # Optional, coerced from int
    category_id: str | None    # Optional, coerced from int
    category_code: str | None  # Optional
    brand: str | None          # Optional
    price: Decimal | None      # Optional, coerced from string/int/float, must be >= 0
    user_session: str | None   # Optional
```

**Key design decisions**:
- `model_config = {"extra": "ignore"}` — API may return fields we don't use; ignore them
  instead of raising errors
- `str_strip_whitespace: True` — Auto-strip whitespace from string fields
- `field_validator("user_id", "product_id", "category_id", mode="before")` — The API
  sometimes returns numeric IDs (e.g., `12345` instead of `"12345"`). The `coerce_ids_to_str`
  validator converts these before Pydantic's type check

**Educational note — Why Pydantic?**:
Pydantic gives us *declarative validation*. Instead of writing:
```python
if "event_time" not in event or not isinstance(event["event_time"], str):
    raise ValueError(...)
try:
    dt = datetime.fromisoformat(event["event_time"])
except ValueError:
    raise ...
```
We just declare `event_time: datetime` with a custom validator. Pydantic handles
parsing, error collection, and serialization automatically.

#### ValidationResult (dataclass)

```python
@dataclass
class ValidationResult:
    total_records: int
    valid_records: int
    invalid_records: int
    validation_errors: list[dict]

    @property
    def success_rate(self) -> float   # valid / total * 100
    @property
    def is_acceptable(self) -> bool   # success_rate >= 95%
```

The `is_acceptable` property enforces that at least 95% of records must pass
validation. If below 95%, `strict_validation=True` in `transform_events()` will
raise `SchemaValidationError`.

#### PipelineMetrics (dataclass)

Collects everything about a pipeline run: row counts, file paths, ADLS upload
status, error details, timing. The `to_dict()` method makes it easy to log as
structured JSON.

---

### 4.3 config.py — Configuration Management

**Purpose**: Load and validate all configuration from environment variables and
`config.env` file.

```python
class ExtractorConfig(BaseSettings):
    model_config = SettingsConfigDict(env_file="config.env", extra="ignore")

    api_base_url: str           # Validated: must start with http:// or https://
    api_key: str
    week_start: date
    week_end: date              # Validated: must be >= week_start
    storage_account: str | None # Must be set with container (both or neither)
    container: str | None
    reject_threshold_pct: float # Default 5.0, range [0, 100]
    api_page_size: int          # Default 1000, max 1000
    ...
```

**Key computed properties**:
```python
@property
def adls_enabled(self) -> bool:
    return storage_account is not None and container is not None

@property
def local_output_file(self) -> Path:
    return local_data_dir / f"week_start={week_start}" / f"week_end={week_end}" / "events.parquet"
```

**Educational note — Pydantic Settings**:
`pydantic_settings.BaseSettings` automatically reads from environment variables.
For example, `api_base_url` maps to `API_BASE_URL` env var. The `env_file="config.env"`
setting loads from a dotenv file as fallback. Validators run automatically:
- `validate_api_url` strips trailing slashes and checks for http/https prefix
- `validate_date_range` ensures week_end >= week_start
- `validate_azure_config` ensures storage_account and container are both set or both None

---

### 4.4 logging_utils.py — Structured Logging

**Purpose**: Produce machine-parseable JSON logs with correlation IDs for tracing
across distributed systems.

#### JSONFormatter

Converts every log record into a JSON line:
```json
{
  "timestamp": "2024-01-10T14:30:00+00:00",
  "level": "INFO",
  "logger": "extractor.pipeline",
  "message": "Extract stage complete",
  "correlation_id": "abc-123-def",
  "rows_fetched": 500,
  "duration_ms": 3200
}
```

**How extra fields work**: Python's `logging` module lets you pass `extra={...}`
to `logger.info()`. The formatter iterates over `record.__dict__` and includes
any non-standard attributes. Special handling for:
- `datetime` objects → `.isoformat()`
- Objects with `__dict__` → `str()`
- Non-JSON-serializable values → `str()` fallback

#### CorrelationIdFilter

A `logging.Filter` that attaches a correlation ID to every log record. This is
critical for tracing: when you have hundreds of log lines from concurrent pipeline
runs, the correlation ID lets you filter for one specific run.

```python
# Generate once at pipeline start
cid = CorrelationIdFilter.generate_correlation_id()  # UUID

# Every subsequent log line automatically includes it
logger.info("Starting extract")  # → {"correlation_id": "abc-123", ...}
```

#### log_operation Context Manager

```python
with log_operation(logger, "extract_phase", week="2024-01-08"):
    # Automatically logs "Starting extract_phase" with context
    do_work()
    # On success: logs "Completed extract_phase" with duration_ms
    # On failure: logs "Failed extract_phase" with duration_ms + error + traceback
```

**Why this pattern?**: It eliminates boilerplate. Without it, every operation would
need `start_time = time()`, `try/except/finally`, and manual duration calculation.

---

### 4.5 extraction.py — API Data Fetch

**Purpose**: Fetch events from the source API with pagination, retry, and rate limiting.

#### Session Setup

```python
def create_http_session(config):
    session = requests.Session()
    retry_strategy = Retry(
        total=config.api_max_retries,        # Default 3
        status_forcelist=[429, 500, 502, 503, 504],  # Retry these HTTP codes
        allowed_methods=["GET"],             # Only retry safe methods
        backoff_factor=1,                    # 1s, 2s, 4s between retries
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
```

**Educational note — Retry Strategy**:
- `429 Too Many Requests` — API rate limit hit; backing off is correct
- `500, 502, 503, 504` — Server errors that may be transient
- `allowed_methods=["GET"]` — Never retry POST/PUT/DELETE (could cause duplicates)
- `backoff_factor=1` — Exponential backoff: 1s, 2s, 4s (prevents thundering herd)

#### Pagination Logic

```python
while True:
    response_data = fetch_page(session, url, params, headers, timeout)

    # Handle different API response formats
    if isinstance(response_data, dict):
        matched_key = next((k for k in ("data", "events", "results", "items") if k in response_data), None)
        if matched_key:
            page_events = response_data[matched_key]
        else:
            raise APIResponseError("missing expected data key")

    all_events.extend(page_events)

    # Stop conditions (in priority order):
    # 1. API reports total count and we've fetched all
    # 2. Empty page received
    # 3. Partial page (fewer records than page_size)
    if len(page_events) < page_size:
        break

    page += 1
    time.sleep(rate_limit_delay)  # Respect API rate limits
```

**Why multiple stop conditions?**: Different APIs signal "no more pages" differently:
- Some return a `meta.total` field
- Some return an empty list on the last+1 page
- Some return a partial page (e.g., 73 records when page_size=1000)

The code handles all three because we don't control the API's pagination behavior.

#### Tenacity Retry Decorator

```python
@retry(
    retry=retry_if_exception_type(requests.exceptions.ConnectionError),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=30),
)
def fetch_page(...):
```

This is a **second layer** of retry on top of urllib3's retry. It catches transient
connection errors (DNS failures, socket resets) that urllib3 doesn't retry.

---

### 4.6 transformation.py — Validation & Normalization

**Purpose**: Validate raw API data against the schema, normalize it into a pandas
DataFrame, and run quality checks.

#### Validation Pipeline

```
raw events (list[dict])
    │
    ├── EventRecord.model_validate(event)  ──── success ──→ valid_events[]
    │
    └── PydanticValidationError caught  ──── failure ──→ rejected_events[]
                                                          (with error details)
```

**What gets rejected?**:
- Missing required fields (`event_time`, `event_type`, `user_id`)
- Invalid date formats (`"not-a-date"`)
- Empty `user_id` after stripping
- Invalid types that can't be coerced

**Strict validation gate**: If `success_rate < 95%`, the entire batch is rejected
with `SchemaValidationError`. This prevents garbage-in-garbage-out — if more than
5% of records fail validation, something is fundamentally wrong with the data source.

#### Normalization

```python
def normalize_events(events, config):
    df = pd.DataFrame(events)
    df["batch_week_start"] = config.week_start.isoformat()  # Add batch metadata
    df["batch_week_end"] = config.week_end.isoformat()
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True)  # Ensure UTC
    df["price"] = pd.to_numeric(df["price"], errors="coerce")      # Decimal → float
    df = df[OUTPUT_COLUMNS]  # Enforce column order
```

**Why separate validation from normalization?**: Validation operates on individual
records (Pydantic). Normalization operates on the entire batch (pandas). This
separation makes each easier to test and reason about.

#### Quality Checks

```python
def run_data_quality_checks(df, config, min_records=1):
    checks = {}

    # 1. Minimum record count (default: at least 1 record)
    if len(df) < min_records:
        checks["min_records"] = {"passed": False}

    # 2. No null user_ids (should have been caught by validation, defense in depth)
    null_user_ids = df["user_id"].isna().sum()

    # 3. Event times within expected week range
    out_of_range = ((df["event_time"] < week_start) | (df["event_time"] >= week_end)).sum()

    # 4. No negative prices
    negative_prices = (df["price"] < 0).sum()

    if not checks["passed"]:
        raise DataQualityError("Data quality checks failed", details=checks)
```

**Educational note — Defense in Depth**: Even though Pydantic validates `price >= 0`,
the quality check re-verifies it on the DataFrame. This catches edge cases like
data corruption during normalization or bugs in the validation layer.

---

### 4.7 storage.py — Local & Cloud Storage

**Purpose**: Write validated data to parquet files and optionally upload to ADLS.

#### Parquet Output

```python
def write_parquet(df, output_path, compression="snappy"):
    ensure_dir(output_path.parent)
    df.to_parquet(output_path, index=False, compression=compression, engine="pyarrow")
    file_hash = sha256_file(output_path)  # Integrity verification
    return {"path": str(output_path), "size_bytes": file_size, "sha256": file_hash}
```

**Why Parquet?**: Columnar format optimized for analytics. Compared to CSV:
- 5-10x smaller file size (Snappy compression)
- Type preservation (dates, decimals stay typed — no CSV parsing ambiguity)
- Predicate pushdown in query engines (Synapse, Spark)

**Why SHA256?**: File integrity check. If the upload to ADLS fails silently or
partially, comparing hashes reveals corruption.

#### ADLS Upload

```python
def upload_to_adls(local_file, config):
    credential = DefaultAzureCredential()  # AAD token auth
    service_client = DataLakeServiceClient(account_url, credential)
    file_client = fs_client.get_file_client(adls_path)
    with local_file.open("rb") as f:
        file_client.upload_data(f, overwrite=True, length=file_size)
```

**Error handling hierarchy**:
- `ClientAuthenticationError` → `AuthenticationError` (credential issue)
- `ServiceRequestError`, `AzureError` → `AzureStorageError` (infrastructure issue)

#### save_events (Orchestrator)

```python
def save_events(df_valid, df_rejects, config, upload_to_cloud=True):
    result["local"] = write_parquet(df_valid, config.local_output_file)
    result["rejects_local"] = write_rejects_parquet(df_rejects, config.local_rejects_output_file)
    if upload_to_cloud and config.adls_enabled:
        result["adls"] = upload_to_adls(config.local_output_file, config)
        if result["rejects_local"]:
            result["rejects_adls"] = upload_rejects_to_adls(...)
```

**Why store rejects?**: Rejects are valuable for debugging. If a customer complains
about missing data, you can look at the rejects parquet to see exactly which records
failed and why.

---

### 4.8 checkpoint.py — State Management

**Purpose**: Track which week the pipeline has processed, so it knows which week
to process next.

#### JSON State (Mirror/Backup)

```python
def write_json_state(config, state):
    config.state_file.write_text(json.dumps(state, indent=2))

def read_json_state(config):
    return json.loads(config.state_file.read_text())
```

The JSON file is a **mirror** of the DB checkpoint — never the source of truth.
If the DB is unavailable, the pipeline fails (by design, to prevent data drift).

#### Pending DB Updates (JSONL Queue)

```python
def append_pending_db_update(config, payload):
    with config.pending_updates_file.open("a") as f:
        f.write(json.dumps(payload) + "\n")
```

**Why JSONL?**: JSON Lines format (one JSON object per line) supports append-only
writes. Unlike a single JSON array, you don't need to read-modify-write the entire
file to add an entry.

**Atomic rewrite for cleanup**:
```python
def rewrite_pending_db_updates(config, remaining):
    temp_file = pending_file.parent / f"{pending_file.name}.tmp"
    with temp_file.open("w") as f:
        for item in remaining:
            f.write(json.dumps(item) + "\n")
    temp_file.replace(pending_file)  # Atomic on POSIX, near-atomic on Windows
```

Write to a temp file first, then rename. This prevents data loss if the process
crashes mid-write.

---

### 4.9 azure_sql.py — Database Integration

**Purpose**: Azure SQL as the source of truth for checkpoint state and pipeline
observability.

#### AAD Token Authentication

```python
def _get_sql_access_token_bytes():
    cred = DefaultAzureCredential()
    token = cred.get_token("https://database.windows.net/.default").token
    token_bytes = token.encode("utf-16-le")
    return len(token_bytes).to_bytes(4, "little") + token_bytes
```

**Why AAD tokens instead of passwords?**:
- No passwords stored in code or config files
- Tokens auto-rotate (short-lived)
- Works with managed identities in Azure (zero-secret deployment)
- The `SQL_COPT_SS_ACCESS_TOKEN = 1256` constant tells the ODBC driver to use
  token auth instead of SQL auth

#### Checkpoint CRUD

**Read** (`db_get_checkpoint`):
```sql
SELECT pipeline_name, last_successful_week_start, last_successful_week_end,
       next_week_start, next_week_end, updated_at_utc
FROM dbo.pipeline_checkpoint
WHERE pipeline_name = ?
```

**Upsert** (`db_update_checkpoint`):
```sql
MERGE dbo.pipeline_checkpoint AS target
USING (SELECT ? AS pipeline_name) AS source
ON target.pipeline_name = source.pipeline_name
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

**Why MERGE?**: Atomic upsert. Without it, you'd need `SELECT` + `IF EXISTS` +
`UPDATE/INSERT`, which has race conditions in concurrent scenarios.

#### Pipeline Run Tracking

Three tables provide full observability:

```
dbo.pipeline_checkpoint     # What week to process next
dbo.pipeline_run            # One row per pipeline execution
dbo.pipeline_stage          # One row per stage within a run
```

**Example data flow**:
```
pipeline_run: {run_id: "abc-123", status: "STARTED", batch_week: "2024-01-08..14"}
  └── pipeline_stage: {stage_name: "EXTRACT", status: "SUCCESS", row_count: 500}
  └── pipeline_stage: {stage_name: "TRANSFORM", status: "SUCCESS", row_count: 495}
  └── pipeline_stage: {stage_name: "LOAD", status: "SUCCESS", row_count: 495}
  └── pipeline_stage: {stage_name: "QUALITY_GATE", status: "PASSED"}
pipeline_run: {run_id: "abc-123", status: "SUCCESS", rows_output: 495, reject_rate: 1.0%}
```

#### Pending Update Replay

```python
def flush_pending_db_updates(config):
    pending = read_pending_db_updates(config)
    remaining = []
    for item in pending:
        try:
            db_update_checkpoint(config, ...)  # Replay
        except Exception:
            remaining.append(item)  # Keep for next time
    rewrite_pending_db_updates(config, remaining)
```

**Why partial replay?**: If you have 3 pending updates and #2 fails, you don't
want to lose #1 (already replayed) or #3 (not yet tried). The code tries each
independently and keeps only the failures.

---

### 4.10 pipeline.py — Orchestration Engine

**Purpose**: Wire all components together for both full-pipeline and per-stage execution.

#### Full Pipeline Flow

```python
def run_pipeline(config, skip_if_exists, upload_to_cloud, dry_run):
    metrics = PipelineMetrics()

    # 1. Register run in DB (fail-fast if DB unreachable)
    run_id = db_insert_pipeline_run(config, pipeline_name, metrics)

    try:
        # 2. Skip check (idempotency)
        if skip_if_exists and check_output_exists(config):
            return metrics  # Already processed this week

        # 3. EXTRACT (with stage tracking)
        raw_events = fetch_events(config)

        # 4. TRANSFORM (with stage tracking)
        df_valid, df_rejects = transform_events(raw_events, config)

        # 5. LOAD (with stage tracking, skip if dry_run)
        save_result = save_events(df_valid, df_rejects, config)

        # 6. QUALITY GATE
        if reject_rate > threshold:
            raise DataQualityError(...)

    finally:
        # 7. Always finalize run in DB (even on failure)
        db_finalize_pipeline_run(config, run_id, metrics)
```

#### Stage Runners (for Airflow Multi-Task DAG)

Each stage reads from the previous stage's metadata file and writes its own:

```
_stage_extract_{run_tag}.json       →  raw events path, row count
_stage_transform_{run_tag}.json     →  validated path, reject stats
_stage_load_{run_tag}.json          →  output paths, ADLS status
_stage_quality_gate_{run_tag}.json  →  PASSED/FAILED
```

**Why `{run_tag}`?**: Prevents collision when multiple DAG runs execute concurrently.
Each Airflow run gets a unique `run_id` that becomes the tag.

#### Quality Gate Logic

```python
def decide_run_status(reject_rate_pct, rows_rejected, threshold_pct):
    if rows_rejected <= 0:
        return "SUCCESS"                    # Perfect run
    if reject_rate_pct <= threshold_pct:
        return "SUCCESS_WITH_REJECTS"       # Some rejects, within tolerance
    return "FAILED"                         # Too many rejects
```

**Why three statuses?**:
- `SUCCESS` = no issues
- `SUCCESS_WITH_REJECTS` = checkpoint advances, but rejects are logged for review
- `FAILED` = checkpoint does NOT advance, pipeline raises error, Airflow retries

---

### 4.11 cli.py — Command-Line Interface

**Purpose**: Parse arguments, coordinate checkpoint logic, dispatch to pipeline or
stage runners.

#### Argument Parsing

```bash
# Full pipeline (backward compatible)
python -m extractor.cli

# With explicit dates
python -m extractor.cli --week-start 2024-01-08 --week-end 2024-01-14

# With DB checkpoint (Airflow mode)
python -m extractor.cli --use-checkpoint

# Single stage (Airflow multi-task DAG)
python -m extractor.cli --use-checkpoint --stage extract
python -m extractor.cli --use-checkpoint --stage transform
python -m extractor.cli --use-checkpoint --stage load
python -m extractor.cli --use-checkpoint --stage quality-gate
python -m extractor.cli --use-checkpoint --stage salesforce-sync

# Flags
python -m extractor.cli --dry-run          # Validate only, don't write files
python -m extractor.cli --force            # Ignore skip-if-exists
python -m extractor.cli --local-only       # Skip ADLS upload
python -m extractor.cli --advance-on-skip  # Move checkpoint even if output exists
python -m extractor.cli --check-api        # Health check and exit
```

#### Checkpoint Update Logic (Post-Run)

```python
def _update_checkpoint(config, metrics, args, pipeline_name):
    was_skipped = (metrics.rows_fetched == 0) and check_output_exists(config)
    ok_to_advance = metrics.run_status in {"SUCCESS", "SUCCESS_WITH_REJECTS"}

    if ok_to_advance and (not was_skipped or args.advance_on_skip):
        next_week_start = config.week_end + timedelta(days=1)
        next_week_end = compute_week_end(next_week_start)

        # Try DB write
        try:
            db_update_checkpoint(config, ...)
        except Exception:
            # DB failed — queue for replay
            append_pending_db_update(config, {...})

        # Always sync JSON mirror
        write_json_state(config, {...})
```

**Why `advance_on_skip`?**: If the pipeline skips because output already exists,
you may still want to advance the checkpoint to avoid re-processing the same week
forever. This flag lets Airflow operators control this behavior.

#### Return Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Any error (config, checkpoint, data quality, API, etc.) |
| 130 | Interrupted by user (Ctrl+C) |

---

### 4.12 salesforce_sync.py — CRM Integration

**Purpose**: Push aggregated customer metrics from Azure SQL to Salesforce.

#### Flow

```
Azure SQL (vw_salesforce_customer_weekly_export)
    │
    ├── fetch_export_data()  →  list of {customer_id, spend, activity, tier}
    │
    └── push_to_salesforce()  →  Salesforce Bulk API upsert
         │
         ├── Map DB columns to SF field names
         │   external_customer_id  →  External_Customer_Id__c
         │   total_spend_week      →  Total_Spend_Week__c
         │   ...
         │
         └── sf.bulk.Customer_Weekly_Metric__c.upsert(records, "External_Signal_Id__c")
```

**Bulk API**: Instead of one API call per record (which would hit Salesforce API
limits), the bulk API handles up to 10,000 records per batch.

**Error Rate Threshold**: If more than 10% of records fail to upsert, the entire
sync is considered failed:
```python
if error_count / len(records) > 0.10:
    raise ExtractorError("Salesforce sync error rate too high")
```

---

## 5. Design Patterns Explained

### Pattern 1: DB-First Checkpoint

```
                      ┌─────────────┐
                      │  Azure SQL  │  ← Source of Truth
                      │  checkpoint │
                      └──────┬──────┘
                             │
                      ┌──────┴──────┐
                      │  JSON File  │  ← Mirror/Backup
                      │  state.json │
                      └─────────────┘
```

**Why not just use a file?**: Files can get out of sync in distributed systems.
If two Airflow workers process the same week simultaneously, a file-based checkpoint
has race conditions. A database with `MERGE` provides atomic upserts.

**Why keep the JSON file?**: As a human-readable backup. If the DB is migrated or
corrupted, the JSON file serves as a recovery point.

### Pattern 2: Best-Effort Writes with Pending Queue

```
Pipeline succeeds → Try DB checkpoint write
    │
    ├── Success → Done
    │
    └── DB unavailable → Save to pending_db_updates.jsonl
                              │
                              └── Next pipeline run → flush_pending_db_updates()
                                      │
                                      ├── Replay succeeds → Remove from file
                                      └── Still failing → Keep in file
```

**Why not fail the pipeline if DB write fails?**: The data extraction was successful.
Failing the pipeline would mean re-extracting data unnecessarily. The checkpoint
update can be replayed later without data loss.

### Pattern 3: Inter-Stage Data Passing via Sidecar Files

```
Stage 1 (Extract):
  Writes: _raw_events.json, _stage_extract_{tag}.json

Stage 2 (Transform):
  Reads:  _stage_extract_{tag}.json → finds _raw_events.json path
  Writes: _validated_events.parquet, _stage_transform_{tag}.json

Stage 3 (Load):
  Reads:  _stage_transform_{tag}.json → finds _validated_events.parquet path
  Writes: events.parquet, _stage_load_{tag}.json
```

**Why not pass data through Airflow XCom?**: XCom is designed for small metadata
(< 48KB in default Airflow). Our data files can be megabytes. File-based passing
is more efficient and doesn't bloat the Airflow metadata DB.

### Pattern 4: Idempotent Execution

```python
if skip_if_exists and check_output_exists(config):
    # Output already exists → skip extraction
    # Optionally re-upload to ADLS (in case previous upload failed)
```

**Why idempotency matters**: Airflow may retry a task that already succeeded (e.g.,
due to worker crash after task completion but before status update). Without
idempotency, you'd process the same week twice, wasting API quota and compute.

---

## 6. Airflow Orchestration

### DAG Structure

```python
with DAG(
    dag_id="reverse_etl_customer_weekly",
    schedule="0 14 * * 1-5",       # Mon-Fri at 2pm UTC
    max_active_runs=1,              # No concurrent runs
    dagrun_timeout=timedelta(hours=4),
    catchup=False,                  # Don't backfill missed runs
):
    extract >> validate_and_transform >> load >> quality_gate >> salesforce_sync
```

### Task Configuration

| Task | Command | Timeout | Purpose |
|------|---------|---------|---------|
| `extract` | `--stage extract` | 2 hours | Fetch from API (can be slow for large datasets) |
| `validate_and_transform` | `--stage transform` | 30 min | CPU-bound validation |
| `load` | `--stage load` | 30 min | Parquet write + ADLS upload |
| `quality_gate` | `--stage quality-gate` | 5 min | Quick check, fails fast |
| `salesforce_sync` | `--stage salesforce-sync` | 30 min | Bulk API call |

### Failure Handling

```python
DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,    # 5min, 10min, 20min
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": on_task_failure,  # Slack alert
}
```

**Failure notification flow**:
```
Task fails → on_task_failure() callback
    │
    ├── Log error with dag_id, task_id, execution_date, log_url
    │
    └── Send Slack webhook (if slack_webhook_url Variable is set)
        POST https://hooks.slack.com/...
        {"text": "ALERT: Task `extract` failed in DAG `reverse_etl_customer_weekly`\n..."}
```

### Docker Compose Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Webserver  │     │  Scheduler   │     │  PostgreSQL   │
│  (Airflow UI)│     │  (DAG exec)  │     │  (Metadata)   │
│  port 8080   │     │              │     │  port 5432    │
│  health: curl│     │  health:     │     │  health: pg   │
│    /health   │     │  jobs check  │     │    isready    │
└──────────────┘     └──────────────┘     └──────────────┘
```

---

## 7. Test Strategy & Coverage Analysis

### Testing Pyramid

```
         /\
        /  \        7 Integration Tests
       / IT \       (real HTTP mocking with responses library,
      /______\       real file I/O, mocked DB)
     /        \
    /  Unit    \    184 Unit Tests
   /  Tests     \   (all external deps mocked)
  /______________\
```

### Test Philosophy

1. **Mock at boundaries**: External services (Azure SQL, ADLS, Salesforce API, source API)
   are mocked. Internal logic uses real objects.
2. **Real file I/O**: Tests use `tmp_path` (pytest fixture) for real filesystem operations.
   This catches path handling bugs that mocks would miss.
3. **No credentials needed**: Every test runs without Azure/Salesforce credentials.

### Integration Tests (test_integration.py)

These tests exercise the **full pipeline flow** end-to-end:

```
HTTP mock (responses library)  →  real fetch_events()  →  real transform_events()
    →  real save_events()  →  real parquet files on disk
```

| Test | What It Proves |
|------|----------------|
| `test_happy_path_full_pipeline` | 20 events → parquet with correct schema, metadata columns |
| `test_mixed_valid_and_invalid_records` | 19 valid + 1 invalid → SUCCESS_WITH_REJECTS, rejects file has error details |
| `test_quality_gate_rejects_above_threshold` | 95% valid but 5% > 3% threshold → DataQualityError raised, files still written |
| `test_empty_api_response` | Empty `data: []` → SchemaValidationError (0% success rate) |
| `test_api_pagination` | 25 events in 3 pages → all 25 in output parquet |
| `test_skip_if_exists` | Pre-existing output → pipeline skips, no API calls |
| `test_stage_runners_end_to_end` | Extract → Transform → Load → Quality Gate sequentially |

### Unit Test Modules

| Module | # Tests | Key Techniques |
|--------|---------|----------------|
| `test_azure_sql.py` | 21 | `MagicMock` for pyodbc connection/cursor, `@patch` for `get_sql_connection` |
| `test_checkpoint.py` | 14 | Real file I/O with `tmp_path`, malformed JSONL handling |
| `test_cli.py` | 29 | `monkeypatch.setattr(sys, "argv", [...])` for arg parsing, `MagicMock` for pipeline |
| `test_config.py` | 9 | Pydantic validation errors, computed property tests |
| `test_extraction.py` | 8 | `responses` library for HTTP mocking, pagination simulation |
| `test_logging_utils.py` | 15 | `logging.LogRecord` construction, JSON parsing of formatter output |
| `test_models.py` | 16 | Pydantic model validation edge cases (coercion, rejection) |
| `test_pipeline.py` | 21 | `@patch` decorators stacked 7 deep, stage metadata roundtrip |
| `test_salesforce_sync.py` | 11 | `MagicMock` for simple-salesforce bulk API, error rate threshold |
| `test_storage.py` | 23 | Real parquet read/write, `@patch` for ADLS client, Azure exceptions |
| `test_transformation.py` | 13 | Mixed valid/invalid events, quality check edge cases |

### Coverage Analysis (90% Overall)

**Modules at 100% coverage**:
- `exceptions.py` — Every exception class instantiated and tested
- `logging_utils.py` — JSON formatter, correlation ID, setup, log_operation
- `salesforce_sync.py` — Config, fetch, push, error threshold

**Modules at 90-95%**:
- `azure_sql.py` (90%) — Uncovered: `_get_sql_access_token_bytes` (AAD token encoding)
- `config.py` (94%) — Uncovered: `get_config()` error path
- `models.py` (94%) — Uncovered: rare edge cases in price coercion
- `storage.py` (95%) — Uncovered: `upload_rejects_to_adls` error paths
- `transformation.py` (91%) — Uncovered: negative price check, out-of-range events
- `extraction.py` (90%) — Uncovered: retry backoff timing, max retries exceeded

**Module at 80%**:
- `pipeline.py` (80%) — Uncovered: nested try/except in DB stage tracking, ADLS
  re-upload on skip path, transform-phase exception handlers. These are **defensive
  error-handling branches** that only trigger when multiple failures occur simultaneously.

**Why 100% isn't the goal**: The uncovered lines are mostly:
1. AAD token encoding (requires real Azure credentials)
2. Nested exception handlers (e.g., "DB stage_end failed while handling a stage failure")
3. Platform-specific file operations (e.g., atomic rename behavior on Windows)

Testing these would add complexity with diminishing returns. The 90% threshold
covers all business logic and normal error paths.

---

## 8. Audit Findings — Detailed Breakdown

### CRITICAL-1: Silent API Response Fallback

**Before** (dangerous):
```python
page_events = response_data.get("items", [])  # Silently returns [] for unknown keys
```

**After** (safe):
```python
_KNOWN_KEYS = ("data", "events", "results", "items")
matched_key = next((k for k in _KNOWN_KEYS if k in response_data), None)
if matched_key is not None:
    page_events = response_data[matched_key]
else:
    raise APIResponseError(
        f"API response missing expected data key. "
        f"Expected one of {_KNOWN_KEYS}, got keys: {list(response_data.keys())}",
    )
```

**Why this was critical**: If the API changed its response format from `{"data": [...]}`
to `{"records": [...]}`, the old code would silently return zero events. The pipeline
would "succeed" with 0 rows — advancing the checkpoint past a week that was never
actually processed. Data would be permanently lost.

### CRITICAL-2: Airflow Jinja in Python Context

**Before** (broken):
```python
"SF_USERNAME": "{{ var.value.sf_username }}",  # Literal string, not rendered!
```

**After** (correct):
```python
"SF_USERNAME": Variable.get("sf_username"),  # Evaluated at DAG parse time
```

**Why this was critical**: Jinja templates are only rendered in **template fields**
(like `bash_command`). The `env={}` parameter of `BashOperator` is a Python dict
evaluated at DAG parse time. The Salesforce task would receive the literal string
`"{{ var.value.sf_username }}"` as the username, causing authentication to fail
every time.

### CRITICAL-3: Hardcoded Docker Credentials

**Before**:
```yaml
POSTGRES_PASSWORD: airflow  # Default password in plain text
```

**After**:
```yaml
POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-airflow}  # From environment, with fallback
```

**Why this was critical**: Anyone with access to the repository could see the
database password. In production, this password controls the Airflow metadata DB
which stores connection credentials, variables, and DAG run history.

### HIGH-1: No Duplicate Run Prevention

**Problem**: Without a unique index, two concurrent DAG runs could process the same
week, creating duplicate data.

**Fix**: Filtered unique index on `pipeline_run`:
```sql
CREATE UNIQUE INDEX UX_pipeline_run_week
ON dbo.pipeline_run (pipeline_name, batch_week_start, batch_week_end)
WHERE status IN ('SUCCESS', 'SUCCESS_WITH_REJECTS');
```

**Why filtered?**: We only prevent duplicate *successful* runs. Failed runs should
be retryable without hitting the constraint.

### HIGH-2: Salesforce Error Rate Threshold

**Problem**: If Salesforce returned errors for 50% of records, the sync would still
report "success" — silently losing data.

**Fix**: 10% threshold. If more than 10% of records fail to upsert, the entire
sync fails with `ExtractorError`, triggering Airflow retry and Slack alert.

### HIGH-3: Stale Checkpoint Detection

**Problem**: If the pipeline stops running (e.g., Airflow scheduler is down), no
one would notice until a stakeholder asks "where's last week's data?"

**Fix**: On startup, check `updated_at_utc` from the checkpoint. If older than 14
days, log a warning:
```python
if age > timedelta(days=14):
    logger.warning("Checkpoint is stale — not updated in over 2 weeks")
```

### HIGH-4: ADLS Re-Upload on Skip

**Problem**: If the pipeline ran successfully but ADLS upload failed, the next run
would skip (output file exists locally) without re-attempting the upload.

**Fix**: When skipping, check if cloud upload was requested and attempt it:
```python
if skip_if_exists and check_output_exists(config):
    if upload_to_cloud and config.adls_enabled:
        upload_to_adls(config.local_output_file, config)
```

### HIGH-5: Stage Metadata Cleanup

**Problem**: `_stage_*.json`, `_raw_events.json`, and `_validated_*.parquet` files
accumulated over time, wasting disk space.

**Fix**: After successful pipeline completion, remove all temporary files:
```python
def cleanup_stage_metadata(config):
    for pattern in ("_stage_*.json", "_raw_events.json", "_validated_*.parquet"):
        for f in output_dir.glob(pattern):
            f.unlink()
```

### MEDIUM Issues (6 fixes)

| # | Issue | Impact | Fix |
|---|-------|--------|-----|
| 9 | Email on failure with no SMTP | Silent notification failure | Disabled, rely on Slack |
| 10 | No exponential backoff | Rapid retries overwhelm API | Added with 30min cap |
| 11 | Only 2 retries | Transient failures not recovered | Increased to 3 |
| 12 | No DAG timeout | Stuck runs never stop | Added 4-hour timeout |
| 13 | Typo `htp://` in example | Copy-paste errors | Fixed to `http://` |
| 14 | SQL `go CREATE` syntax | View creation fails | Fixed to `GO\nCREATE OR ALTER` |

---

## 9. Security Analysis

### Threat Model

| Threat | Mitigation | Status |
|--------|-----------|--------|
| API key exposure | Loaded from `config.env` (gitignored), never logged | Mitigated |
| SQL injection | Parameterized queries (`?` placeholders) | Mitigated |
| Credential in Docker Compose | Externalized to env vars with defaults | Mitigated |
| Azure credential theft | AAD token auth (no passwords), short-lived tokens | Mitigated |
| Salesforce credential exposure | Environment variables, not in code or config files | Mitigated |
| Path traversal in file names | `pathlib.Path` used throughout (no string concatenation) | Mitigated |
| Log injection | Structured JSON logging (values escaped by `json.dumps`) | Mitigated |
| Sensitive data in logs | Exception details dict used instead of f-strings with secrets | Mitigated |
| Unauthorized ADLS access | `DefaultAzureCredential` + RBAC ("Storage Blob Data Contributor") | Mitigated |
| Data tampering detection | SHA256 hash computed for every parquet file | Mitigated |

### OWASP Top 10 Relevance

| OWASP Category | Applicability | Status |
|----------------|--------------|--------|
| A01 Broken Access Control | ADLS/SQL access via AAD | Controlled |
| A02 Cryptographic Failures | No encryption at rest (relies on Azure-managed encryption) | Acceptable |
| A03 Injection | SQL parameterized, no shell commands with user input | Mitigated |
| A04 Insecure Design | Exception hierarchy prevents silent failures | Mitigated |
| A05 Security Misconfiguration | Docker credentials externalized | Mitigated |
| A07 Auth Failures | AAD tokens, no hardcoded passwords | Mitigated |
| A09 Logging & Monitoring | Structured JSON logs, Slack alerts, DB observability | Covered |

---

## 10. Production Readiness Checklist

### Infrastructure Prerequisites

- [ ] Azure SQL Database provisioned
  - Tables created from `SQL_Script/config_tables.sql`
  - View created from `SQL_Script/synapse_script/vw-customer-weekly_signals.sql`
  - Service principal or managed identity with `db_datareader` + `db_datawriter` roles
- [ ] Azure Data Lake Storage Gen2
  - Storage account + container created
  - Service principal with "Storage Blob Data Contributor" role
- [ ] Salesforce
  - Custom object `Customer_Weekly_Metric__c` deployed
  - Custom fields mapped to pipeline output
  - API user with bulk API permissions
- [ ] Network
  - Airflow workers can reach API endpoint, Azure SQL, ADLS, Salesforce
  - Firewall rules allow outbound HTTPS (443) and SQL (1433)

### Configuration

- [ ] `config.env` populated with all required values
- [ ] Airflow Variables set:
  - `sf_username`, `sf_password`, `sf_security_token`, `sf_domain`
  - `slack_webhook_url` (for failure notifications)
- [ ] Docker `.env` file with `POSTGRES_PASSWORD` (not default)
- [ ] ODBC Driver 18 for SQL Server installed on Airflow workers

### Validation

- [ ] All 191 tests pass: `python -m pytest tests/ -v`
- [ ] Coverage at 90%+: `python -m pytest tests/ --cov=extractor`
- [ ] Health check passes: `python -m extractor.cli --check-api`
- [ ] Dry run succeeds: `python -m extractor.cli --week-start 2024-01-08 --week-end 2024-01-14 --dry-run`
- [ ] Full pipeline succeeds: `python -m extractor.cli --week-start 2024-01-08 --week-end 2024-01-14 --local-only`
- [ ] ADLS upload succeeds (non-local mode)
- [ ] Airflow DAG appears in UI and triggers successfully

### Monitoring & Alerting

- [ ] Slack webhook delivers failure notifications
- [ ] `dbo.pipeline_run` shows correct run status and metrics
- [ ] `dbo.pipeline_stage` shows individual stage timing
- [ ] Log aggregation configured (e.g., Azure Monitor, ELK, Datadog)
- [ ] Stale checkpoint alert monitored (14+ days without update)

### Operational Runbook

**Pipeline is stuck on the same week**:
1. Check `dbo.pipeline_checkpoint` for the stuck `next_week_start`
2. Check `dbo.pipeline_run` for recent failures
3. If data quality issue: investigate rejects parquet, adjust `REJECT_THRESHOLD_PCT`
4. If API issue: check API health with `--check-api`
5. Force re-run: `--force --week-start X --week-end Y`

**Pending DB updates accumulating**:
1. Check `state/pending_db_updates.jsonl` for entries
2. If DB is reachable, restart pipeline — `flush_pending_db_updates()` will auto-replay
3. If DB is permanently down, manually update checkpoint after recovery

**Salesforce sync failures**:
1. Check error rate in logs ("Salesforce upsert errors (sample)")
2. Common causes: field mapping changes, permission issues, record lock conflicts
3. If error rate < 10%, sync still succeeds (errors logged as warnings)
4. If error rate > 10%, sync fails — fix root cause and retry

---

*Report generated from codebase analysis covering 1,250 statements across 14 modules,
191 automated tests at 90% code coverage, and 14 production-readiness audit findings.*
