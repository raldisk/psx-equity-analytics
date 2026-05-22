# Security Policy — PSX Analytics (P3)

## Scope

This policy covers the **PSX Market Microstructure Analytics** system:

- `serving/psx_analytics_api.py` — FastAPI read-only serving layer
- `scripts/psx_ingest.py` — manifest-authority ingestion pipeline
- `scripts/duckdb_manager.py` — DuckDB connection management
- `airflow/dags/psx_pipeline_dag.py` — Airflow orchestration
- `dbt/` — staging and mart transformation models
- `infra/docker/` — container build and deployment configuration

**Out of scope:** PSX CSV source data infrastructure, Airflow core, DuckDB engine internals.

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report vulnerabilities privately via email:

```
raldisk@heraldcollamar.com
Subject: [PSX-SECURITY] <brief description>
```

Include: description, reproduction steps, affected component, and severity assessment. You will receive an acknowledgment within 5 business days and a resolution timeline within 10 business days.

## Data Sensitivity

PSX EOD market data is **publicly available** — raw OHLCV figures are not confidential. However:

- The `manifest.json` file is an operational artifact that reveals ingestion timing and file paths; treat it as internal.
- DuckDB files (`.duckdb`) contain derived analytics and should not be exposed externally.
- `.env` files contain deployment secrets (`PSX_AIRFLOW_CONN_ID`, credentials); they must never be committed.

## Known Architectural Security Properties

**Read-only serving layer:** `serving_connection()` opens DuckDB with `read_only=True`. The FastAPI process cannot write to the analytics database — this is a structural guarantee, not a configuration option.

**No authentication by default:** The API is designed for internal/intranet deployment. If exposed publicly, add an API gateway or authentication middleware before the FastAPI process. See `docs/adr/ADR-001-duckdb-over-postgres.md` for the deployment model rationale.

**SARIMA dependency isolation:** `statsmodels` is in the `[stats]` optional extra. If not installed, the pipeline degrades gracefully (`sarima_status = SKIPPED_NO_STATSMODELS`). This limits attack surface from the statistical computing dependency.

## Dependency Scanning

Security CI runs weekly (`security.yml`):

- `bandit` — SAST on `scripts/` and `serving/`
- `safety` — CVE scan of `requirements.txt`
- `trivy` — container image scan

DuckDB version is pinned to `>=0.10.0,<1.0.0`. Upgrade evaluations require a dedicated test pass against the full regression suite before merging.
