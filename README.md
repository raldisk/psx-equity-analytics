![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![DuckDB](https://img.shields.io/badge/DuckDB-0.10-FEE500?style=for-the-badge&logo=duckdb&logoColor=black)
![dbt](https://img.shields.io/badge/dbt--duckdb-1.7.0-FF694B?style=for-the-badge&logo=dbt&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.109-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-2.8.0-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-pyarrow_14.x-50ABF1?style=for-the-badge&logo=apacheparquet&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-22C55E?style=for-the-badge)

> **psx-analytics** — Manifest-authority, memory-bounded microstructure analytics platform for Philippine Stock Exchange EOD equity data with corporate-action versioning and isolated serving.

## Part of the Philippine Financial Data Platform

| Repository | Role | Port |
|---|---|---|
| [econ-intel-platform](https://github.com/raldisk/econ-intel-platform) | Unified intelligence hub — downstream consumer of all enrichment edges | 8001 |
| [ph-macro-lakehouse](https://github.com/raldisk/ph-macro-lakehouse) | Gold-layer macro data pipeline — FX rates and macro indicators via Parquet/S3 | 8000 |
| [psx-equity-analytics](https://github.com/raldisk/psx-equity-analytics) | PSX equity microstructure analytics — VWAP, Amihud illiquidity, SARIMA trend | **8004** |
| [bsp-credit-risk-warehouse](https://github.com/raldisk/bsp-credit-risk-warehouse) | BSP Circular 855 regulatory credit exposure DWH — monthly closed-period serving | 8003 |
| [iso20022-settlement-engine](https://github.com/raldisk/iso20022-settlement-engine) | ISO 20022 pacs.008 interbank settlement ledger — daily bilateral PHP flow serving | 8002 |

Each repository is independently deployable and self-sufficient. Cross-repo data flows are optional enrichment edges — any repository operates fully without its peers.

**This repository** has two platform roles:
- **Producer** → [`econ-intel-platform`](https://github.com/raldisk/econ-intel-platform) optionally calls `GET /analytics/daily` (port 8004 in single-machine dev) for VWAP, Amihud illiquidity, and SARIMA trend enrichment.
- **Consumer** → optionally receives daily interbank settlement PHP flow from [`iso20022-settlement-engine`](https://github.com/raldisk/iso20022-settlement-engine) (`SETTLEMENT_API_URL`). When set, `settlement_php_flow` appears in `fact_daily_analytics`; when absent, the column is NULL and the pipeline continues normally.


---

## Table of Contents

- [Repository Layout](#repository-layout)
- [Architecture](#architecture)
- [Quick Start](#quick-start)
- [Service Endpoints](#service-endpoints)
- [Running the System](#running-the-system)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [Data Contracts / Schema](#data-contracts--schema)
- [Development](#development)
- [Failure Modes](#failure-modes)
- [Tech Stack](#tech-stack)
- [License](#license)

---

## Repository Layout

```
psx-analytics/
├── scripts/
│   ├── psx_ingest.py          ← F-019+F-021+F-024: manifest-based ingest + corporate-action versioning
│   └── duckdb_manager.py      ← F-022+GSR-005: date-range guard + read-only serving connection
├── serving/
│   └── psx_analytics_api.py   ← FastAPI: validate_date_range() enforced at every fact endpoint
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_psx_eod.sql          ← manifest-path source (never glob raw/)
│   │   └── marts/
│   │       └── fact_daily_analytics.py  ← F-023: daily grain VWAP/Amihud; F-025: SARIMA isolation
│   ├── dbt_project.yml
│   └── profiles/profiles.yml.example
├── airflow/
│   └── dags/
│       └── psx_pipeline_dag.py    ← detect → ingest → schema-init → dbt run → DQ assertions
├── tests/
│   └── test_psx_analytics_regression.py  ← F-019/F-022/F-023/F-025/GSR-005 regression suite
├── governance/
│   ├── hardening-log.md
│   └── closure-declaration.md
├── docs/
│   └── architecture.svg               ← Architecture diagram (generated)
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

**Runtime boundaries:** The Airflow scheduler and FastAPI serving layer run as separate processes against the same DuckDB file. DuckDB enforces writer exclusivity: `pipeline_connection()` in `duckdb_manager.py` acquires the exclusive write lock during scheduled pipeline windows; `serving_connection(read_only=True)` is used by FastAPI and supports unlimited concurrent readers. These two code paths never hold the write lock simultaneously — `max_active_runs=1` on `psx_pipeline_dag` enforces this at the orchestration layer.

---

## Architecture

![Architecture Diagram](docs/architecture.svg)

### Key Design Rules

- `manifest.json` is the **sole authority** for which Parquet file is canonical for any `(symbol, session_date)` pair — `stg_psx_eod.sql` reads only manifest-referenced paths, never a glob against `raw/`; PSX amendments update the manifest pointer and retain the superseded file; `rebuild_manifest_from_raw()` recovers state by taking the latest-modified file per key.
- `validate_date_range()` is called **before any DuckDB connection opens** in FastAPI — requests missing `start_date`/`end_date`, spanning more than `PSX_MAX_DATE_RANGE_DAYS` (default 90), or with `end_date < start_date` are rejected HTTP 422 before any query executes; this is a hard memory guard, not a soft advisory.
- Non-additive measures (`VWAP`, `Amihud illiquidity`, `price_impact_bps`) are computed and stored **only at daily grain** in `fact_daily_analytics` — `SUM(VWAP)` across rows is a schema-level impossibility; `fact_trade` contains only additive tick-level measures (price, volume, traded value).
- SARIMA estimation failures are **never fatal to a dbt run** — per-symbol `try/except` wraps the `SARIMAX` fit; non-convergent symbols receive `sarima_status = "FAILED_CONVERGENCE"` with `NULL` trend/seasonal columns; `fact_daily_analytics` is always fully populated.
- The serving connection is **permanently `read_only=True`** — no FastAPI endpoint can acquire the DuckDB write lock regardless of query construction; the pipeline's exclusive write window is enforced by `psx_pipeline_dag`'s sequential task structure.
- `computed/` versioning follows `computed/v{N}/symbol/date/` — corporate action sequence number `N` increments on each adjustment event; prior versions are **never deleted**; `manifest.json` tracks the current canonical `computed_version` per `(symbol, date)` and FastAPI can serve any historical version via `?version=N`.

---

## Quick Start

**Prerequisites:** [Python 3.11](https://www.python.org/downloads/), [Apache Airflow 2.8.0](https://airflow.apache.org/docs/apache-airflow/2.8.0/installation/index.html) (shared with P2 environment if applicable).

```bash
# 1. Configure environment
cp .env.example .env
# Edit .env — set PSX_DATA_ROOT (absolute path, must exist) and PSX_DUCKDB_PATH

# 2. Install dependencies
pip install -r requirements.txt

# 3. Initialize DuckDB schema
python3 -c "from scripts.duckdb_manager import initialize_schema; initialize_schema()"

# 4. Run regression suite (no live PSX data required)
pytest tests/test_psx_analytics_regression.py -v
# Expected: all F-019/F-022/F-023/F-025/GSR-005 tests pass

# 5. Start serving API (2 workers; each holds an independent read-only DuckDB connection)
uvicorn serving.psx_analytics_api:app --host 0.0.0.0 --port 8000 --workers 2

# 6. Verify API health
curl http://localhost:8000/health
# Expected: DuckDB connection status, manifest record count, memory_limit value

# 7. Drop a PSX EOD CSV and trigger pipeline
cp /path/to/ALI_20250115.csv $PSX_DROP_DIR/
airflow dags trigger psx_pipeline_dag
```

> **Warning:** `psx_pipeline_dag` and the FastAPI server must not run simultaneous DuckDB write operations. `pipeline_connection()` acquires an exclusive write lock. A second pipeline instance attempting to acquire this lock will block indefinitely — not error. Set `max_active_runs=1` on `psx_pipeline_dag` before activating the schedule.

---

## Service Endpoints

| Service | URL | Credentials |
|---|---|---|
| FastAPI serving layer | `http://localhost:8000` | None (no auth on analytics endpoints) |
| FastAPI interactive docs | `http://localhost:8000/docs` | None |
| FastAPI health check | `http://localhost:8000/health` | None |
| Airflow UI (shared with P2) | `http://localhost:8080` | `admin` / `$AIRFLOW_ADMIN_PASSWORD` |
| DuckDB file (file-local) | `$PSX_DUCKDB_PATH` | File-local; no network port exposed |

---

## Running the System

### Single pipeline run (manual)

```bash
# Drop a PSX EOD CSV into the monitored drop directory
cp /path/to/SMPH_20250210.csv $PSX_DROP_DIR/

# Trigger pipeline
airflow dags trigger psx_pipeline_dag

# Verify manifest updated for the new session
python3 -c "
import json, os
with open(os.path.join(os.getenv('PSX_DATA_ROOT'), 'manifest.json')) as f:
    m = json.load(f)
print(list(m.items())[:3])
"
```

### Amendment handling (PSX re-delivers a corrected file)

```bash
# Drop corrected file using the same session-date filename
cp /path/to/ALI_20250115_amended.csv $PSX_DROP_DIR/ALI_20250115.csv

# psx_ingest.py detects existing manifest key with mismatched SHA-256,
# records prior_raw_path, updates manifest pointer to new file, sets amended=True.
# Original file is retained in raw/ — no data is deleted.

# Trigger pipeline to reprocess the amended canonical file
airflow dags trigger psx_pipeline_dag
```

### API queries (date range enforced on all fact endpoints)

```bash
# Valid: 7-day VWAP / Amihud range
curl "http://localhost:8000/analytics/daily?symbol=ALI&start_date=2025-01-08&end_date=2025-01-15"

# Valid: historical computed version (corporate-action adjusted)
curl "http://localhost:8000/analytics/daily?symbol=ALI&start_date=2025-01-08&end_date=2025-01-15&version=2"

# Rejected: missing date range → HTTP 422
curl "http://localhost:8000/analytics/daily?symbol=ALI"

# Rejected: range exceeds 90 days → HTTP 422
curl "http://localhost:8000/analytics/daily?symbol=ALI&start_date=2024-01-01&end_date=2025-01-15"
```

### Scheduled runs

`psx_pipeline_dag` runs daily at `07:00 PHT` (after PSX EOD file delivery window). Activation:

```bash
airflow dags unpause psx_pipeline_dag
```

---

## API Reference

> **Read-only.** All endpoints use `serving_connection(read_only=True)`. No write path is exposed. DuckDB `memory_limit` is applied at connection open per `DUCKDB_MEMORY_LIMIT` env var.

| Method | Path | Description | Parameters |
|---|---|---|---|
| `GET` | `/analytics/daily` | `fact_daily_analytics` for a symbol and date range; includes VWAP, Amihud illiquidity, price_impact_bps, and SARIMA-derived columns | Required: `symbol` (string), `start_date` (ISO 8601), `end_date` (ISO 8601). Optional: `version` (integer, computed corporate-action version N) |
| `GET` | `/analytics/trades` | `fact_trade` additive tick-level records for a symbol and date range; max 90-day window enforced | Required: `symbol` (string), `start_date` (ISO 8601), `end_date` (ISO 8601). Max range: `PSX_MAX_DATE_RANGE_DAYS` (default 90) |
| `GET` | `/manifest/{symbol}/{session_date}` | Returns canonical manifest entry for a `(symbol, session_date)` pair | Path: `symbol` (string), `session_date` (ISO 8601 date). Response includes `raw_path`, `computed_version`, `amended` flag, `file_sha256` |
| `GET` | `/health` | Returns DuckDB connection status, manifest record count, and applied `memory_limit` setting | None |

---

## Configuration

```dotenv
# ── Data paths ────────────────────────────────────────────────────────────────
PSX_DATA_ROOT=/data/psx                    # Absolute path; directory must exist before first run
PSX_DUCKDB_PATH=/data/psx/psx.duckdb      # DuckDB file; created by initialize_schema() on first run
PSX_DROP_DIR=/data/psx/drop               # Airflow FileSensor watches this directory for new CSVs

# ── DuckDB resource limits ────────────────────────────────────────────────────
DUCKDB_MEMORY_LIMIT=2GB                    # Applied at every connection open (SET memory_limit)
PSX_MAX_DATE_RANGE_DAYS=90                # F-022 hard guard; override for admin queries only

# ── dbt ───────────────────────────────────────────────────────────────────────
DBT_PROJECT_DIR=/opt/airflow/dbt           # Mounted into Airflow container
DBT_PROFILES_DIR=/opt/airflow/dbt/profiles

# ── FastAPI ───────────────────────────────────────────────────────────────────
API_HOST=0.0.0.0
API_PORT=8000
API_WORKERS=2                              # Each Uvicorn worker opens an independent read-only connection
```

---

## Data Contracts / Schema

### `manifest.json` (manifest authority, F-019)

```yaml
key: (symbol, session_date)              # canonical dedup key; one entry per trading session per symbol
fields:
  raw_path: string                       # absolute path to canonical Parquet file in raw/
  prior_raw_path: string | null          # path to superseded file when amended=True
  file_sha256: string                    # SHA-256 of canonical raw_path; mismatch triggers amendment flow
  amended: boolean                       # True when PSX delivered a correction for this (symbol, session_date)
  computed_version: integer              # current canonical computed/ version (increments on corporate action)
  computed_path: string                  # absolute path: computed/v{N}/symbol/date/
  ingested_at: ISO8601                   # millisecond precision; prevents same-second collision
recovery: rebuild_manifest_from_raw() — scans raw/, takes latest-modified Parquet per key
idempotency: re-delivering same file (identical SHA-256) → amended=False, no manifest update
```

### `fact_daily_analytics` (non-additive daily grain, F-023)

```yaml
grain: one row per (symbol_key, session_date_key)
non_additive_measures:
  - vwap: total_value / total_volume — computed at daily grain only; NOT summable across days
  - amihud_illiquidity: |daily_return| / daily_volume
  - price_impact_bps: (high - low) / close × 10000
additive_measures:
  - total_volume, total_value, trade_count
sarima_columns:
  - trend_component, seasonal_component: NULL when sarima_status != 'SUCCESS'
  - sarima_status: enum [SUCCESS, FAILED_CONVERGENCE, INSUFFICIENT_DATA, SKIPPED_NO_STATSMODELS]
quality_gates:
  - price > 0 enforced at stg_psx_eod layer (dbt assertion)
  - volume >= 0 enforced at stg_psx_eod layer (dbt assertion)
  - manifest-path source enforced in stg_psx_eod.sql (never glob raw/)
```

### `manifest.json` recovery path

```yaml
trigger: manifest.json missing or corrupted
command: python3 -c "from scripts.psx_ingest import rebuild_manifest_from_raw; rebuild_manifest_from_raw()"
mechanism: scans raw/, takes latest-modified Parquet per (symbol, session_date) key
data_loss: none — all Parquet files in raw/ are always retained (never deleted on amendment)
```

---

## Development

### Without Airflow (scripts only)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

export PSX_DATA_ROOT=/tmp/psx_dev PSX_DUCKDB_PATH=/tmp/psx_dev/dev.duckdb
mkdir -p /tmp/psx_dev/raw /tmp/psx_dev/drop

python3 -c "from scripts.duckdb_manager import initialize_schema; initialize_schema()"
python3 -c "from scripts.psx_ingest import ingest_psx_csv; ingest_psx_csv('tests/fixtures/ALI_20250115.csv')"
```

### Tests

| Suite | Command | Coverage |
|---|---|---|
| Full hardening regression | `pytest tests/test_psx_analytics_regression.py -v` | F-019/F-021/F-022/F-023/F-025/GSR-005 |
| With coverage report | `pytest tests/test_psx_analytics_regression.py -v --cov=scripts --cov=serving` | All modules |
| FastAPI date-range guards only | `pytest tests/test_psx_analytics_regression.py -v -k "api"` | F-022 guard paths |

### Lint

```bash
ruff check scripts/ serving/ && \
black --check scripts/ serving/
```

---

## Failure Modes

| Symptom | Cause | Fix |
|---|---|---|
| `ValueError: date range exceeds 90 days` from FastAPI | Client sent unguarded full-history query without date bounds | Add `start_date`/`end_date` to request; set `PSX_MAX_DATE_RANGE_DAYS` env override only for verified admin ops |
| DuckDB `IO Error: Could not set lock on file` | Two pipeline instances running concurrently, both attempting the write lock | Set `max_active_runs=1` on `psx_pipeline_dag`; terminate the second instance; DuckDB file integrity is preserved on clean process exit |
| `fact_daily_analytics.vwap` returns nonsensical aggregates | VWAP computed at tick grain (F-023 regression) | Verify `stg_psx_eod.sql` selects from manifest path, not raw glob; re-run `dbt run --select fact_daily_analytics` only |
| SARIMA columns are all NULL for all symbols | `statsmodels` not installed in the dbt Python model environment | Install `statsmodels>=0.14.0` in the dbt runner environment; `sarima_status = SKIPPED_NO_STATSMODELS` is expected and non-fatal without it |
| Manifest shows `amended=False` after PSX correction delivery | Second file has identical content SHA-256 to original (non-substantive re-delivery) | Correct behavior — idempotent ingest by design; if PSX confirms substantive correction, request a file with verifiably different content |
| `manifest.json` missing after disk event | Disk failure or accidental deletion | Run `rebuild_manifest_from_raw()` — recovers state from `raw/` directory; no Parquet data is lost because raw files are never deleted |
| `fact_daily_analytics` missing symbols after corporate action | `computed/` version mismatch; manifest pointing to pre-adjustment computed version | Run `create_computed_version()` for affected symbols to increment version; update manifest; re-run `dbt run --select fact_daily_analytics` |
| FastAPI `503` under high concurrent load | Workers using `pipeline_connection()` instead of `serving_connection()` | Verify all FastAPI endpoint handlers call `serving_connection(read_only=True)`; DuckDB `read_only=True` supports unlimited concurrent readers without contention |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Python 3.11 (`psx_ingest.py`) + `manifest.json` authority |
| Analytics DB | DuckDB 0.10 (file-local; `memory_limit=2GB`) |
| Data format | Parquet (pyarrow 14.x backend) |
| Transform | dbt-core 1.7.0 + dbt-duckdb 1.7.0 |
| SARIMA forecasting | statsmodels 0.14 (optional; per-symbol fault isolation) |
| Serving | FastAPI 0.109 + Uvicorn 0.27 |
| Orchestration | Apache Airflow 2.8.0 (shared with P2) |
| Testing | pytest 8.x + pytest-cov + httpx (FastAPI test client) |
| Timezone | pendulum 3.x (Asia/Manila) |

---

## License

MIT — see [`LICENSE`](LICENSE).
