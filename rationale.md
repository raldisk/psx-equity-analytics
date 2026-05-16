# P3 — Design Rationale: PSX Market Microstructure Analytics

> Scope: P3 (`psx-analytics`) only. Decisions, trade-offs, and rejected alternatives for the PSX EOD equity data analytics platform.

---

## Manifest-Based Deduplication (F-019)

The original design used SHA-256 at the file level to prevent duplicate ingestion. This correctly prevents re-ingestion of the same physical file, but fails against a realistic PSX operational pattern: PSX occasionally delivers an amended file for the same `(symbol, date)` trading session — a new CSV with corrected prices or volumes. A new file has a different SHA-256 despite representing the same logical period. Without a canonical tracking layer, `raw/` accumulates two Parquet files for the same key, and DuckDB's wildcard query returns duplicates in every aggregate.

`manifest.json` resolves this by tracking exactly one `raw_path` per `(symbol, session_date)`. When `ingest_psx_csv()` encounters an existing manifest key with a different SHA-256, it records the prior path in `prior_raw_path`, updates `raw_path` to the new file, and sets `amended=True`. The prior file is retained — `raw/` is append-only — but the manifest is the only source of truth for which file to query. The dbt staging model's `stg_psx_eod.sql` reads from the manifest-referenced path, never from a glob.

The recovery contract is explicit: if `manifest.json` is lost (disk event, accidental deletion), `rebuild_manifest_from_raw()` scans `raw/` and reconstructs the manifest by taking the latest-modified Parquet per key. This is a full recovery, not a data loss event — all raw Parquet files are preserved.

---

## Corporate-Action Versioning (F-024)

Without versioning, a stock split or rights offering requires regenerating all historical `computed/` outputs. Prior to F-024's fix, a researcher's backtesting result from pre-split data would be irreproducible after the corporate action adjustment because the `computed/` directory had been overwritten.

`create_computed_version()` writes adjusted outputs to `computed/v{N}/symbol/date/` where `N` is the corporate action sequence number. The manifest tracks the current canonical `computed_version` and `computed_path` per key. Prior versions are retained indefinitely. FastAPI exposes `?version=N` on `/analytics/daily` to serve any historical version.

This approach was selected over a timestamp-based versioning scheme because corporate action sequence numbers are semantically meaningful to analysts — `v2` unambiguously means "after the second corporate action event" — whereas timestamps are opaque. Sequence numbers also allow deterministic cross-symbol version alignment when a market-wide adjustment affects multiple symbols simultaneously.

---

## SARIMA Isolation per Symbol (F-025)

The original implementation ran SARIMAX as a single dbt Python model across all symbols. A non-convergent symbol (common for illiquid PSX equities with sparse trading days) raised an exception that aborted the entire dbt run, blocking all marts for all symbols.

The fix wraps each symbol's SARIMAX fit in a per-symbol `try/except`. Non-convergent symbols receive `sarima_status = "FAILED_CONVERGENCE"` and `NULL` in the `trend_component`/`seasonal_component` columns. The full `fact_daily_analytics` table is always populated; SARIMA columns are selectively NULL rather than globally absent. The dbt run never fails due to a single symbol's convergence failure.

`statsmodels` is declared optional in `requirements.txt` — symbols processed without it receive `sarima_status = "SKIPPED_NO_STATSMODELS"`. This allows the platform to operate in analytics mode (all non-SARIMA measures available) even in environments where `statsmodels` cannot be installed.

---

## 90-Day Date Range Guard (F-022)

DuckDB's in-process analytics engine has no query-level resource limiter. An unguarded full-table scan of `fact_trade` (the tick-grain table, potentially millions of rows) executes within the FastAPI worker process. Memory exhaustion crashes the worker without recovery, taking down all serving capacity.

`validate_date_range()` enforces a mandatory date range predicate on all fact-table queries before any DuckDB connection is opened. The function raises `ValueError` (surfaced as HTTP 422) for: absent `start_date` or `end_date`, range exceeding `PSX_MAX_DATE_RANGE_DAYS` (default 90), invalid date format, or `end_date < start_date`. The 90-day default is configured via environment variable, allowing administrative override for backfill operations without a code change.

A server-side memory limit of 2 GB (`SET memory_limit = '2GB'`) is applied at every connection open as a secondary defence. The `validate_date_range()` guard is the primary enforcement mechanism; the memory limit is defence-in-depth for queries that pass date validation but still produce large intermediate results.

---

## Pipeline/Serving Connection Isolation (GSR-005)

DuckDB is a single-writer database. A concurrent write and read against the same database file will cause the write connection to block until the read connection closes, or raise a lock acquisition error. In a pipeline-plus-serving architecture, this creates a reliability risk: a long-running dbt transform would block all serving connections for its duration.

Two connection factories enforce strict separation. `pipeline_connection()` acquires an exclusive write lock and is called only by the Airflow DAG's pipeline tasks during scheduled windows. `serving_connection(read_only=True)` acquires no write lock and supports unlimited concurrent readers. FastAPI workers exclusively use `serving_connection()`. Schema DDL (`initialize_schema()`) is separated into a startup function called once by the pipeline before any data tasks; DDL and DML never compete for the lock within the same pipeline run.

The consequence is that the pipeline and the serving layer cannot conflict at the DuckDB layer. The Airflow DAG's `max_active_runs=1` setting prevents concurrent pipeline instances from competing for the write lock with each other.
