# P3-ADVERSARIAL-AUDIT-TEMPLATE — PSX Analytics

**Purpose:** Structured adversarial review template for P3 PSX Analytics. Complete before any major release or significant architectural change. Reviewer must attempt to find failures, not confirm correctness.

**Project:** P3 — PSX Market Microstructure Analytics  
**Audit scope:** Ingestion pipeline, DuckDB serving layer, dbt models, API endpoints

---

## Section 1 — Data Contract Attacks

### 1.1 Manifest Bypass
**Attack:** Submit a `staging_source_list.json` that contains a glob path (`data/raw/SM_*.parquet`) instead of a canonical path. Does `stg_psx_eod.sql` pass it to `read_parquet()` verbatim?

**Expected defense:** F-019 enforcement — the pipeline DAG (`generate_manifest_source_list` task) resolves all paths through `manifest.json` before writing `staging_source_list.json`. Glob paths are never written.

**Evidence required:** Show the DAG task code that resolves paths. Show that `staging_source_list.json` format is `[{canonical_raw_path: ".../<exact filename>.parquet"}]`, not a glob.

### 1.2 Amendment Injection
**Attack:** Write two entries to `manifest.json` for the same `(symbol, session_date)` key — one with `amended: false` and one with `amended: true`. Does the pipeline ingest both, producing duplicate rows in `fact_daily_analytics`?

**Expected defense:** Manifest key format `{symbol}_{session_date}` is unique by construction. A second write to the same key overwrites the first — Python dict semantics. No duplicate keys possible.

**Evidence required:** Show `psx_ingest.py` key generation logic. Show that `load_manifest()` returns a `dict` (not a list).

### 1.3 Future-Date Injection
**Attack:** Submit a Parquet file with `session_date = current_date + 30 days`. Does it propagate to `fact_daily_analytics`?

**Expected defense:** `stg_psx_eod.sql` `validated` CTE filters `session_date <= current_date + INTERVAL '1 day'`.

**Evidence required:** Run `SELECT COUNT(*) FROM stg_psx_eod WHERE session_date > current_date + INTERVAL '1 day'` against a test DB seeded with a future-dated row. Result must be 0.

---

## Section 2 — Concurrency Attacks

### 2.1 Simultaneous Pipeline + Serving Query
**Attack:** Start a long-running `SELECT COUNT(*) FROM fact_daily_analytics` via the API while `psx_ingest.py` is mid-write (use DuckDB's simulated write delay). Does the serving connection block indefinitely?

**Expected defense:** `serving_connection()` opens with `read_only=True`. DuckDB's MVCC allows read-only connections to proceed concurrently with a write connection's transaction.

**Evidence required:** Verify DuckDB version `>=0.10.0` MVCC behavior for concurrent read_only + write connections. Cite DuckDB release notes.

### 2.2 Double-Write Race
**Attack:** Trigger two simultaneous Airflow DAG runs for the same `execution_date`. Does `pipeline_connection()` in the second run block or fail?

**Expected defense:** `max_active_runs=1` in the Airflow DAG prevents concurrent runs for the same DAG. Verify in DAG definition.

**Evidence required:** Show `max_active_runs=1` in `psx_pipeline_dag.py`.

---

## Section 3 — API Attacks

### 3.1 Missing Date Range (F-022 bypass)
**Attack:** Call `/analytics/SM` with no `from`/`to` parameters. Does the handler open a DuckDB connection and execute a full-table scan?

**Expected defense:** `validate_date_range()` is called before `serving_connection()` in every endpoint handler. Missing dates → `HTTP 422` before any DuckDB query opens.

**Evidence required:** Grep `serving/psx_analytics_api.py` for every endpoint function. Confirm `validate_date_range()` is called before `serving_connection()` in each. No exceptions.

### 3.2 Inverted Date Range
**Attack:** Call `/analytics/SM?from=2024-12-31&to=2024-01-01`. Does the API return an empty result set (silent failure) or a clear error?

**Expected defense:** `validate_date_range()` raises `HTTPException(400)` when `from_date > to_date`.

**Evidence required:** Test with `curl`. Confirm `HTTP 400`, not `HTTP 200` with empty `data`.

### 3.3 Oversized Date Range
**Attack:** Call with a 365-day range when `PSX_MAX_DATE_RANGE_DAYS=90`.

**Expected defense:** `validate_date_range()` raises `HTTPException(400)` when range exceeds `PSX_MAX_DATE_RANGE_DAYS`.

---

## Section 4 — Dependency Attacks

### 4.1 SARIMA Without statsmodels
**Attack:** Uninstall `statsmodels` and trigger a pipeline run.

**Expected defense:** Pipeline completes with `sarima_status = SKIPPED_NO_STATSMODELS` for all rows. No crash. No partial write.

**Evidence required:** Run `pip uninstall statsmodels -y && airflow tasks run psx_pipeline compute_sarima <date>`. Check `fact_daily_analytics.sarima_status`.

### 4.2 DuckDB Version Drift
**Attack:** Upgrade DuckDB to `1.0.0` (above the `<1.0.0` pin).

**Expected defense:** `pip install` respects the pin and refuses the upgrade. CI fails if the pin is violated.

---

## Audit Sign-Off

| Section | Reviewer | Date | Result |
|---|---|---|---|
| 1 — Data Contract | | | |
| 2 — Concurrency | | | |
| 3 — API | | | |
| 4 — Dependencies | | | |

All sections must be ✅ PASS before the release tag is pushed.
