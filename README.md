# Weather Data Pipeline — API → S3 Data Lake

A production-style scheduled batch pipeline that ingests live hourly weather data for five global cities from the Open-Meteo API, stores raw JSON responses in AWS S3 partitioned by date, normalizes the data into flat CSV files, and writes processed data back to S3 for downstream analytics consumption.

---

## Problem Statement

Static flat files like CSVs are snapshots — the moment they leave a source system they begin going stale. Operational and analytical systems need **fresh, continuously updated data** stored in a durable, queryable location that multiple downstream consumers can access without re-calling the source API.

This pipeline solves three problems that did not exist in a simple CSV-to-database pipeline:

**Data freshness** — a scheduler triggers the pipeline daily, always fetching the previous day's complete hourly dataset. Data is never more than 24 hours stale.

**Durable raw storage** — the exact API response is preserved in S3 before any transformation occurs. If normalization logic changes or a bug is discovered, historical data can be reprocessed from the raw layer without re-calling the API.

**Fault isolation** — one city failing API ingestion never stops the other four from being processed. The pipeline always completes with a clear record of what succeeded and what failed.

---

## Architecture

![Architecture Diagram](docs/architecture.svg)

**S3 Folder Structure (Hive partitioning format):**
```
weather-pipeline-raw-{name}/
├── raw/
│   └── year=2024/month=01/day=15/
│       ├── london.json
│       ├── accra.json
│       ├── paris.json
│       ├── los_angeles.json
│       └── singapore.json
└── processed/
    └── year=2024/month=01/day=15/
        ├── london.csv
        ├── accra.csv
        ├── paris.csv
        ├── los_angeles.csv
        └── singapore.csv
```

---

## Tech Stack

| Tool | Role | Why |
|---|---|---|
| Python 3.11+ | Pipeline language | Industry standard for data engineering |
| requests | HTTP client | Simple, reliable API calls with timeout control |
| boto3 | AWS SDK | Official Python interface to S3 and all AWS services |
| pandas | Data transformation | DataFrame operations for JSON normalization |
| pyyaml | Config management | Human-readable configuration — separates config from code |
| python-dotenv | Credentials management | Keeps AWS secrets out of source code and version control |
| tenacity | Retry logic | Declarative exponential backoff on transient API failures |
| APScheduler | Pipeline scheduling | Lightweight scheduler without full Airflow infrastructure |
| AWS S3 | Raw + processed storage | Durable, cheap, infinitely scalable object storage |

---

## Project Structure

```
weather_pipeline/
├── config/
│   ├── config.yaml          # Cities, API settings, S3 config (non-sensitive)
│   └── config_loader.py     # Loads YAML once at import, exposes typed constants
├── src/
│   ├── ingest.py            # API requests, retry logic, fault isolation
│   ├── storage.py           # All S3 operations: write_raw, read_raw, write_processed
│   ├── transform.py         # JSON normalization, column enrichment, unit suffixes
│   └── scheduler.py         # APScheduler config, daily trigger, missed run handling
├── logs/                    # Pipeline run logs (auto-created, named by run_id)
├── .env                     # AWS credentials (never committed)
├── .gitignore
└── main.py                  # Pipeline orchestrator and entry point
```

---

## Setup

**Prerequisites:** Python 3.11+, uv, AWS account with S3 access

**1. Clone the repository**
```bash
git clone <repo-url>
cd weather_pipeline
```

**2. Install dependencies**
```bash
uv install
```

**3. Create AWS resources**

Create an IAM user with `AmazonS3FullAccess` and generate access keys. Create an S3 bucket in your preferred region. See [documentation](docs/documentation.md) for the full IAM least-privilege setup.

**4. Configure credentials**
```bash
cp .env.example .env
```

Edit `.env`:
```
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1
S3_BUCKET=weather-pipeline-raw-yourname
```

**5. Configure pipeline settings**

Edit `config/config.yaml` to set your S3 bucket name, cities, and weather variables. Credentials never go in this file.

**6. Run the pipeline manually**
```bash
uv run main.py
```

**7. Start the scheduler**
```bash
uv run src/scheduler.py
```

The scheduler runs the pipeline daily at 01:00 UTC. Press Ctrl+C to stop.

**Manual backfill for a specific date:**
```python
from main import run_pipeline
from datetime import date
run_pipeline(target_date=date(2024, 1, 15))
```

---

## Verifying Results

After a successful run, verify in your S3 console:

**File count:** 10 files per run — 5 raw JSON + 5 processed CSV in their respective partitioned folders.

**CSV structure:** Each processed CSV should have exactly 24 rows (one per hour) and 10 columns: `city_name`, `date`, `timestamp`, `ingested_at`, `temperature_2m_c`, `relative_humidity_2m_pct`, `wind_speed_10m_kmh`, `wind_direction_10m_deg`, `visibility_m`, `precipitation_mm`.

**Idempotency test:** Run the pipeline twice on the same date. File contents should be identical after both runs — S3 overwrites with the same data, no duplicates created.

**Pipeline summary log:**
```
PIPELINE COMPLETE
  Run ID            : a3f2b1c4
  Target date       : 2024-01-15
  Duration          : 12.43s
  Cities successful : 5/5
  Cities failed     : none
  Total rows loaded : 120 (expected: 120)
  Dataset location  : s3://weather-pipeline-raw-.../processed/year=2024/month=01/day=15/
```

Total rows is the key health metric — always expect `cities_successful × 24`.

---

## Observability

Every run produces a structured log file in `logs/` named with the run ID:
```
logs/pipeline_a3f2b1c4.log
```

Log format:
```
2024-01-15 01:00:01 | run=a3f2b1c4 | INFO  | src.ingest    | Fetching london for 2024-01-14
2024-01-15 01:00:02 | run=a3f2b1c4 | INFO  | src.storage   | Raw JSON written → s3://bucket/raw/...
2024-01-15 01:00:03 | run=a3f2b1c4 | WARN  | src.ingest    | Retry 1/3 for paris — connection timeout
2024-01-15 01:00:06 | run=a3f2b1c4 | INFO  | src.ingest    | Successfully fetched paris — 24 hours
```

---

## Key Design Decisions

See [documentation](docs/documentation.md) for full reasoning. Summary:

- **Raw storage before transformation** — preserves ground truth for reprocessing and debugging
- **Hive partition format** — enables partition pruning in Athena, Spark, and all major query engines
- **Fault isolation per city** — one failure never stops others; pipeline always completes
- **Exponential backoff retries** — backs off progressively to avoid worsening API rate limits
- **YAML configuration** — separates config from code; non-engineers can modify cities and variables without touching Python
- **Sequential processing** — acceptable for 5 cities; designed to be replaced with concurrent processing at scale

---

## What I Would Do Differently in Production

**Orchestration** — replace APScheduler with Apache Airflow. Each pipeline stage becomes a DAG task with retries, backfill UI, dependency management, and SLA alerting.

**Concurrency** — replace the sequential city loop with `asyncio` or `ThreadPoolExecutor`. At 500+ cities, sequential processing becomes a bottleneck. Concurrent processing reduces runtime from minutes to seconds.

**Query layer** — add AWS Athena pointed at the processed S3 folder. Athena auto-discovers Hive partitions and enables SQL queries directly against S3 without loading data into a database.

**Data quality** — add Great Expectations checks on row counts, null rates, and value ranges before writing to the processed layer.

**Alerting** — add SNS or PagerDuty integration to the scheduler's `on_job_error` listener so failures page an engineer immediately rather than being discovered the next morning.

**Secrets management** — replace `.env` files with AWS Secrets Manager. Credentials rotate automatically and are never stored on developer machines.