# ADR-001 · DuckDB over PostgreSQL for Analytical Serving

**Status:** Accepted  
**Date:** 2024-Q1 (Phase 3)  
**Deciders:** P3 architecture review

---

## Context

PSX Analytics requires a local analytical data store for:

- Storing ~90 days × ~250 symbols × daily OHLCV + derived metrics (Amihud, VWAP, SARIMA trend)
- Serving read-only analytical queries from a FastAPI layer
- Running dbt transformations (staging → mart) on a scheduled basis

The system is deployed in a single-node environment collocated with an Airflow 2.8.0 instance. There is no operational team to manage a database server.

## Decision

Use **DuckDB** as the analytical store in place of PostgreSQL or any server-based RDBMS.

## Rationale

**Volume fits DuckDB's model.** PSX EOD data at full 90-day window is approximately 250 symbols × 90 days × ~500 bytes per row = ~11 MB of raw fact data. DuckDB handles this entirely in memory during query execution; no distributed query planning is needed.

**No server process to manage.** DuckDB is an embedded library. Deployment is a single `.duckdb` file. There is no `pg_hba.conf`, no `postgresql.conf`, no `pg_ctl`, no connection pooling configuration, and no version-upgrade migration process beyond the DuckDB package pin.

**Column-oriented storage matches the query pattern.** Analytical queries over `fact_daily_analytics` filter on `session_date` and `symbol_key`, then aggregate price/volume columns. DuckDB's columnar storage and vectorized execution give order-of-magnitude better performance than row-oriented PostgreSQL for this pattern.

**read_only serving is trivially enforced.** `duckdb.connect(path, read_only=True)` is a single flag. The serving layer structurally cannot write to the analytics database. Implementing equivalent isolation in PostgreSQL requires role management, `REVOKE` grants, and a separate connection string — all operational surface area.

**dbt-duckdb integration is mature.** `dbt-duckdb` (version-pinned `>=1.7.0,<2.0.0`) supports DuckDB as a first-class dbt adapter. The staging → mart transformation pipeline runs entirely inside DuckDB without an ETL framework.

## Trade-offs and Mitigations

**Single-writer constraint.** DuckDB allows only one write connection at a time. The pipeline DAG uses `pipeline_connection()` exclusively for writes and the serving layer uses `serving_connection(read_only=True)`. These two paths never run concurrently by DAG design. If concurrent writes become necessary (multi-DAG ingest), this decision must be revisited.

**No replication.** DuckDB does not support read replicas or streaming replication. The backup/restore scripts (`scripts/backup.sh`, `scripts/restore.sh`) provide point-in-time recovery. This is acceptable given the data source (PSX CSV) is always available to rebuild from scratch.

**WAL orphan risk on SIGKILL.** See `KNOWN_ISSUES.md` KI-003. Mitigated by monitoring via the DuckDB Prometheus exporter.

**Version pin.** DuckDB is pinned to `>=0.10.0,<1.0.0`. The `<1.0.0` upper bound is conservative — the 1.0 release introduced API changes that require validation before adoption. Update the pin after running the full regression suite.

## Alternatives Considered

**PostgreSQL:** Rejected. Operational overhead (server process, role management, connection pooling) is disproportionate to the data volume and team size. The analytical query pattern is a poor fit for row-oriented storage.

**SQLite:** Rejected. Row-oriented; no columnar compression; no DuckDB-style vectorized execution; no native Parquet read. Not suitable for analytical workloads.

**MotherDuck (DuckDB cloud):** Rejected. External dependency; PSX market data must remain on-premise per portfolio data governance policy.
