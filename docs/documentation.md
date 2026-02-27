# Technical Documentation — Weather Data Pipeline

This document records the engineering decisions made during the design and implementation of the API → S3 weather pipeline. It explains not just what the code does, but why each decision was made, what alternatives were considered, and what would change at production scale.

---

## Table of Contents

1. [Why This Pipeline Exists](#why-this-pipeline-exists)
2. [Architecture Decisions](#architecture-decisions)
3. [Configuration Design](#configuration-design)
4. [Ingestion Design](#ingestion-design)
5. [Storage Design](#storage-design)
6. [Transformation Design](#transformation-design)
7. [Scheduling Design](#scheduling-design)
8. [Observability Design](#observability-design)
9. [Scale Considerations](#scale-considerations)
10. [Known Limitations](#known-limitations)

---

## Why This Pipeline Exists

This pipeline represents the second fundamental pattern in data engineering — ingesting live data from an external API into a durable storage layer on a schedule, rather than processing a static file once.

The key differences from a CSV-to-database pipeline:

**The source is live and external.** An API is a third-party system with `rate limits`, `authentication requirements`, `schema evolution`, and `availability incidents`. The pipeline must be defensive about all of these in ways a static file never requires.

**The data must be preserved before transformation.** With a CSV you control the source, if transformation logic changes, you still have the original file. With an API, historical data may be unavailable, rate-limited, or costly to re-fetch. Raw storage is not optional — it is the foundation of reprocessability.

**The pipeline must run unsupervised on a schedule.** A one-time script does not need `retry logic`, `backfill handling`, or `missed run detection`. A scheduled pipeline that runs at 1am without human oversight needs all three.

---

## Architecture Decisions

### Why a Data Lake Over a Database

**We don't control the source schema.** Open-Meteo can add weather variables, rename fields, or restructure the response at any time. A database with a strict schema would reject new fields or fail on renamed ones. S3 stores exactly what arrived — any schema changes are handled downstream when the data is read and processed.

**Multiple consumers with different needs.** A data lake stores raw data once and lets different consumers apply different schemas when they read it. A data scientist might want hourly temperature as a time series. A BI analyst might want daily aggregates by city. A ML engineer might want all variables as a feature matrix. One raw storage layer serves all of them without requiring separate ETL pipelines for each.

**Cost at scale.** Storing years of hourly weather data for five cities in PostgreSQL requires a running server. Storing the same data in S3 costs fractions of a cent per month and requires no server management.

### Why Bronze Layer Architecture

The pipeline implements a two-layer storage pattern:

**Raw (Bronze)** — exact API response, unmodified, stored as JSON. This is the immutable ground truth. It represents what the source system said at a specific point in time.

**Processed (Silver)** — normalized, flattened, enriched CSV. This is derived from Bronze and can always be regenerated from it. If normalization logic improves, you reprocess Bronze — you never re-call the API.

This separation is the foundation of the Bronze/Silver/Gold lakehouse architecture used at Databricks, Airbnb, Netflix, and most modern data platforms. Bronze/Silver is implemented here. Gold (business-level aggregations) would be the next layer downstream.

### Why S3 Over Local File Storage

S3 provides durability guarantees (99.999999999% — eleven nines), automatic replication across availability zones, fine-grained access controls, versioning, lifecycle policies, and native integration with every AWS analytics service. A local filesystem provides none of these. In any production context, local file storage for pipeline outputs is not a serious option.

---

## Configuration Design

### Why YAML Over Python Config Files

Configuration is data, not code. Separating them has three benefits:

**Non-engineers can modify configuration.** A data analyst can add a new city to `config.yaml` without understanding Python. They cannot safely modify a Python module.

**Environment portability.** Different environments (development, staging, production) can use different config files — different bucket names, different city lists, different retry settings — without changing application code.

**Industry standard.** Airflow, Kubernetes, dbt, Great Expectations, and Docker Compose all use YAML for configuration. Building the habit of YAML-based config is building toward professional tooling.

### Why Load Config Once At Import Time

Config is loaded once when `config_loader.py` is first imported, not on demand each time a module needs a value. The reasoning:

**Fail fast.** If the config file is missing or malformed, the error should surface the moment the pipeline starts — not halfway through when the third module tries to read a value. Loading at import time means a broken config file kills the pipeline before a single API call is made.

**Performance.** Five file reads and YAML parse operations per pipeline run versus one is a meaningless difference for a daily batch job, but the principle — don't repeat expensive operations unnecessarily — applies at scale.

**`validate_config()` is called explicitly from `main.py`** rather than automatically at import time. This separation means config values can be imported in tests without triggering credential validation — which is useful when unit testing transformation logic that doesn't need real AWS credentials.

### Credentials Separation

Non-sensitive configuration lives in `config.yaml` — city coordinates, API URLs, S3 bucket name, weather variables, partition format. These can be committed to version control safely.

Sensitive credentials live in `.env` — AWS access key ID, AWS secret access key. These are loaded by `python-dotenv` and never appear in any committed file. The `.gitignore` enforces this.

This separation follows the principle of least exposure — credentials are stored in the minimum number of places with the minimum number of people having access.

---

## Ingestion Design

### Why Separate `_fetch_with_retry` From `fetch_city`

The `@retry` decorator from tenacity wraps the entire function it decorates. If retry logic lived inside `fetch_city`, the retry loop would wrap city name lookup, parameter construction, response validation, and logging — not just the HTTP call.

Separating `_fetch_with_retry` means tenacity retries exactly one thing: the HTTP request to the API. Everything else happens once, outside the retry loop. This makes retry behavior predictable and the logs clean.

### Why Exponential Backoff

Three retry strategies exist:

**No retry** — one failure ends the pipeline for that city. Unacceptable for transient network issues that would succeed on a second attempt.

**Fixed interval retry** — retry every 2 seconds. Simple but potentially harmful. If the API is rate-limiting because of too many requests, retrying every 2 seconds continues to send requests at the same rate — potentially making the rate limiting worse.

**Exponential backoff** — retry after 2s, then 4s, then 8s. Each retry waits twice as long as the previous. This gives an overloaded or rate-limited API progressively more time to recover between attempts. It is the industry standard for retry logic against external APIs for this reason.

### Why Retry Only Transient Errors

Not all failures are worth retrying. The `_is_retryable_error` function distinguishes:

**Retryable:** Network timeouts, connection failures, HTTP 429 (rate limit), HTTP 500/502/503/504 (server errors). These are transient — the same request might succeed moments later.

**Not retryable:** HTTP 400 (bad request — our parameters are wrong, retrying sends the same wrong request), HTTP 401/403 (authentication failure — retrying with the same credentials won't help), HTTP 404 (not found — retrying won't create the resource).

Retrying non-retryable errors wastes time and can cause confusion — a pipeline that retries a 400 error three times before failing looks like a network problem when it's actually a configuration problem.

### Fault Isolation — One City, One Failure

`fetch_all_cities` processes cities in a sequential loop. Each city's result is independent. If Tokyo's API call fails after all retries, the exception is caught, logged, and the loop continues to the next city.

This is the dead letter queue pattern applied at the city level rather than the row level. The pipeline always completes. The summary log always reports exactly which cities succeeded and which failed. An engineer reviewing the logs the next morning has a complete picture immediately.

The alternative — halting the pipeline on first city failure — would mean that a Tokyo API outage prevents London, Accra, Paris, and Singapore data from being collected. That is a disproportionate response to a single city failure.

---

## Storage Design

### Why Store Raw JSON Before Transforming

Three reasons, each independently sufficient:

**Reprocessability.** Transformation logic will change — new columns added, new derived metrics, bug fixes. When logic changes, you want to reprocess historical data using the new logic. If only the normalized CSV exists, reprocessing requires re-calling the API for every historical date. If the raw JSON exists, reprocessing reads from S3 — fast, free, no API dependency.

**Debugging.** When normalized output looks wrong, you need to compare against what the API actually returned. The raw JSON is the ground truth. Without it, you cannot distinguish between "the API returned wrong data" and "our transformation code has a bug."

**Schema drift protection.** APIs change their response format without warning. If normalization fails because the API restructured its response, the raw JSON is still safely stored. Fix the transformation code and reprocess — no data is lost.

### Why Hive Partition Format

The partition structure `year=2024/month=01/day=15` uses the Hive partitioning convention — key=value folder names. This is not arbitrary. Query engines including AWS Athena, Apache Spark, Apache Hive, and Presto all natively understand this format and use it for **partition pruning** — skipping entire folder hierarchies that don't match a query's date filter.

A query for January 2024 data in Athena never reads February or March folders. A query for a specific day reads only that day's folder. Without Hive partitioning, every query scans every file regardless of the date filter.

The format is configuration-driven via `PARTITION_FORMAT` in `config.yaml`. Changing from daily to hourly partitioning requires one config change — no Python code changes.

### Why `io.StringIO` For CSV Writing

Writing a DataFrame to S3 as CSV requires converting it to bytes. Two approaches:

**Write to disk first** — `df.to_csv("temp.csv")`, read the file, upload to S3, delete the temp file. Requires managing temporary files, handling write permissions, and cleanup logic.

**Write to in-memory buffer** — `io.StringIO()` creates a file-like object in memory. `df.to_csv(buffer)` writes CSV to memory. The bytes are uploaded to S3 directly. No temporary files, no cleanup, no disk I/O.

`io.StringIO` is the standard pattern for in-memory file operations in Python pipelines. It appears in any context where you need to write data to a service that expects a file without touching the filesystem.

### S3 Idempotency

S3 uses last-write-wins semantics. Writing to a key that already exists silently overwrites the existing object. This is the natural idempotency mechanism for S3-based pipelines — running the pipeline twice on the same date produces identical files with no special handling required.

This contrasts with Project 1's database upsert, which required explicit `ON CONFLICT DO UPDATE` SQL. S3 handles it automatically.

---

## Transformation Design

### Why `pd.DataFrame(hourly_dict)` For Flattening

The API returns parallel arrays:
```json
{"time": [t1, t2, ...], "temperature_2m": [v1, v2, ...]}
```

Passing a dict of equal-length lists directly to `pd.DataFrame()` aligns them by index automatically, producing one row per index position. This is exactly the normalization operation needed — no manual zipping, no loops, no index management.

This is the most important pandas pattern for API data ingestion. Most APIs return data in this columnar format because it compresses better over the wire than row-oriented JSON.

### Why Unit Suffixes In Column Names

`temperature_2m_c` is self-documenting. An analyst reading this column name knows immediately that the unit is Celsius. `temperature_2m` requires consulting external documentation to determine the unit.

Self-documenting column names reduce the cognitive load on every downstream consumer and eliminate a class of unit confusion bugs — analysts accidentally mixing Celsius and Fahrenheit, or meters and kilometers, in calculations.

Special characters in unit strings (`°`, `%`, `/`) are mapped to clean ASCII suffixes (`_c`, `_pct`, `_kmh`) because special characters in column names cause issues in SQL, some CSV parsers, and many downstream analytical tools.

### Why `city_name` Is Added During Transformation

The raw API response contains coordinates but not a city name. City name is added as a metadata column during transformation rather than in the storage layer because enrichment is transformation's responsibility — storage writes what it receives without modification.

This is also critical for downstream usability. When five cities' CSVs are loaded together for analysis, `city_name` is the join key that makes rows distinguishable. Without it, the combined dataset has no way to filter by city.

---

## Scheduling Design

### Why APScheduler Over Airflow

Apache Airflow is the production standard for pipeline orchestration. APScheduler is used here for two reasons:

**Infrastructure complexity.** Airflow requires a metadata database, a web server, a scheduler process, and optionally worker processes. Setting it up correctly takes significant time and introduces dependencies that would distract from the data engineering concepts this project is teaching.

**Scale appropriateness.** For a single pipeline running once per day, Airflow's full infrastructure is significant overhead. APScheduler provides scheduling, retry handling, and missed run detection in a single Python library with no external dependencies.

The correct production decision is Airflow. The correct learning decision is APScheduler. Knowing the difference — and being able to articulate it — is more valuable than just using Airflow without understanding why.

### Why `misfire_grace_time=3600`

If the scheduler process is down at 01:00 UTC — machine restarted, process crashed — APScheduler detects the missed run when it restarts. `misfire_grace_time=3600` means: if the missed run is within the last hour, execute it immediately. If it's older than one hour, log it as missed and wait for the next scheduled run.

Beyond one hour, manual backfill is required:
```python
run_pipeline(target_date=date(2024, 1, 15))
```

This is why `run_pipeline` accepts an explicit `target_date` parameter rather than always computing yesterday. Backfill capability requires the ability to target any specific date.

---

## Observability Design

### Run ID Per Execution

Every pipeline run generates a UUID-based run ID at startup. This ID appears in every log line and in the log filename. When debugging a failure across multiple scheduled runs, grep for the run ID to see exactly one execution's complete story.

### Total Rows As Health Metric

The pipeline summary reports `total_rows` — the sum of rows across all successfully processed cities. The expected value is always `cities_successful × 24`. Any deviation indicates a problem:

- 96 rows instead of 120 → one city failed somewhere in the pipeline
- 0 rows → all cities failed, likely an API outage or credential issue
- 144 rows → impossible with upsert/overwrite semantics, indicates a bug

This single number gives an engineer reviewing logs immediate signal about pipeline health without reading individual stage logs.

---

## Scale Considerations

### Sequential Processing Becomes A Bottleneck

The current implementation processes cities sequentially — one API call completes before the next begins. For 5 cities averaging 400ms per request, total ingestion time is approximately 2 seconds. Acceptable for a daily batch job.

At 500 cities: 500 × 400ms = 200 seconds of sequential API calls. At 5,000 cities: over 30 minutes. Sequential processing becomes the pipeline's bottleneck.

The solution is concurrent processing using Python's `asyncio` with `aiohttp` for async HTTP requests, or `ThreadPoolExecutor` for thread-based concurrency:

```python
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(fetch_city, city) for city in CITIES]
    results = [f.result() for f in futures]
```

10 concurrent workers reduces 500-city ingestion from 200 seconds to approximately 20 seconds. The tradeoff is increased complexity in error handling and the need to respect API rate limits across concurrent requests.

### Partition Strategy At Scale

Daily partitioning is correct for this dataset — one folder per day, five files per folder. At higher ingestion frequency (hourly data from thousands of sensors), daily partitions become too coarse — a single day's folder might contain millions of files.

Hourly partitioning (`year=/month=/day=/hour=`) distributes files more evenly and improves query performance for time-range queries narrower than a day.

The partition format is externalized to config specifically to make this change a configuration update rather than a code change.

---

## Known Limitations

**No API authentication.** Open-Meteo's free tier requires no authentication. Production weather APIs (Tomorrow.io, WeatherAPI) require API keys with usage limits. The pipeline would need credential management for API keys in addition to AWS credentials.

**No data quality checks on processed output.** The pipeline validates that the API responded successfully but does not verify that temperature values are within plausible ranges, that 24 hours of data were returned (not 23 or 25), or that no variables contain unexpected nulls. Great Expectations would provide these checks.

**No Athena integration.** The processed S3 folder uses Hive partitioning but no Glue Data Catalog table is created. Without a catalog entry, Athena cannot discover the data automatically. Adding a Glue crawler or creating the table manually would enable SQL queries directly against the processed layer.

**Single region.** All data is stored in one AWS region. A multi-region setup with S3 replication would provide geographic redundancy and lower latency for consumers in different regions.

**No file size optimization.** Each city produces a small CSV file per day. After a year, the processed folder contains 1,825 small files (5 cities × 365 days). Small files are inefficient for Spark and Athena — they prefer fewer, larger files. A compaction job that periodically merges small files into larger Parquet files would improve query performance significantly at scale.