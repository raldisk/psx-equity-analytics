# ADR-002 · Airflow as the Sole Scheduling Authority

**Status:** Accepted  
**Date:** 2024-Q1 (Phase 3)  
**Deciders:** P3 architecture review

---

## Context

PSX Analytics requires scheduled execution of:

- Daily ingestion (`psx_ingest.py`) — fetch and manifest PSX EOD CSV data
- dbt staging run (`stg_psx_eod`) — transform raw Parquet to staging view
- dbt mart run (`fact_daily_analytics`) — compute derived metrics including SARIMA
- Governance checks — validate manifest integrity, detect orphaned WAL files

The system is collocated with an Airflow 2.8.0 instance already used by a related P2 project.

## Decision

Use **Airflow DAG** (`airflow/dags/psx_pipeline_dag.py`) as the exclusive scheduling mechanism. No crontab, no systemd timer, no Celery beat.

## Rationale

**Single scheduling authority prevents dual-trigger conflicts.** If both Airflow and a crontab entry schedule `psx_ingest.py`, concurrent execution becomes possible. DuckDB's single-writer constraint means the second trigger will fail at `pipeline_connection()` acquisition, potentially leaving a partial ingestion state. A single scheduler eliminates this class of failure.

**Airflow provides idempotency guarantees.** DAG run state (success/failure/retry) is persisted in the Airflow metadata database. `psx_pipeline_dag.py` uses `depends_on_past=False` with a `max_active_runs=1` constraint, ensuring at-most-one concurrent pipeline execution. Crontab provides no equivalent guarantee.

**Observability is already available.** Airflow's task log UI, task duration tracking, and SLA miss detection are available without additional tooling. Adding crontab or systemd would require separate log aggregation to achieve equivalent visibility.

**P2 reuse.** The Airflow 2.8.0 instance is shared with the P2 project. Operational knowledge, alerting, and on-call procedures already exist for Airflow in this environment. Adding a second scheduling mechanism would fragment operational responsibility.

## Trade-offs

**Airflow process must be running.** If the Airflow scheduler is stopped, PSX ingestion stops. This is acceptable: the data source (PSX CSV) retains historical data, and the pipeline is designed to backfill missed runs via Airflow's `catchup=True` mechanism.

**Airflow overhead for a simple pipeline.** The full Airflow stack (scheduler, webserver, metadata DB) is heavy relative to a simple cron job. This overhead is absorbed because the Airflow instance already exists for P2; it is not added solely for P3.

## Items Explicitly Not Proposed

Audit items 35, 36, 37 (cron/systemd scheduling) were evaluated and rejected. Creating a `crontab` entry or systemd timer for PSX Analytics would introduce a second scheduling authority in conflict with the Airflow DAG. This decision is recorded here to prevent re-proposal in future phases.
