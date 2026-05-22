# P3-RUNBOOK — PSX Analytics Operational Runbook

**Project:** P3 — PSX Market Microstructure Analytics  
**Version:** Phase 3  
**Owner:** raldisk@heraldcollamar.com

---

## 1. Service Overview

| Component | Path | Role |
|---|---|---|
| FastAPI serving layer | `serving/psx_analytics_api.py` | Read-only API, port 8000 |
| Airflow DAG | `airflow/dags/psx_pipeline_dag.py` | Orchestrates daily ingestion |
| psx_ingest.py | `scripts/psx_ingest.py` | Ingest + manifest update |
| duckdb_manager.py | `scripts/duckdb_manager.py` | Connection factory |
| DuckDB file | `${PSX_DATA_ROOT}/psx_analytics.duckdb` | Analytical store |
| manifest.json | `${PSX_DATA_ROOT}/manifest.json` | Path authority |

---

## 2. Daily Health Check

```bash
# 1. Confirm Airflow DAG ran successfully
airflow dags list-runs -d psx_pipeline --state success --limit 1

# 2. Check WAL presence (should be empty at rest)
ls "${PSX_DATA_ROOT}/psx_analytics.duckdb.wal" 2>/dev/null && echo "WAL PRESENT — investigate" || echo "WAL OK"

# 3. Confirm API is responsive
curl -s http://localhost:8000/health | python3 -m json.tool

# 4. Check Prometheus target is up
curl -s http://localhost:9090/api/v1/targets | python3 -m json.tool | grep "psx-analytics"
```

---

## 3. Incident Response

### 3.1 DuckDB WAL Lock (KI-003)

**Symptom:** `duckdb_wal_present = 1` in Prometheus for > 5 minutes; API queries may return 500.

**Resolution:**

```bash
# Stop all processes touching the DuckDB file
airflow dags pause psx_pipeline
# Identify the locking process
lsof "${PSX_DATA_ROOT}/psx_analytics.duckdb"
# Gracefully stop it (SIGTERM, not SIGKILL)
kill -TERM <pid>
# Verify WAL is gone
ls "${PSX_DATA_ROOT}/psx_analytics.duckdb.wal" 2>/dev/null || echo "WAL cleared"
# Resume
airflow dags unpause psx_pipeline
```

### 3.2 Pipeline Failure — Manifest Corruption

**Symptom:** Airflow task `validate_manifest` fails with `KeyError` or JSON parse error.

**Resolution:**

```bash
# Inspect manifest
python3 -m json.tool "${PSX_DATA_ROOT}/manifest.json" | head -40

# If corrupt, restore from last known-good backup
./scripts/restore.sh "${PSX_DATA_ROOT}/backups/psx_analytics_<timestamp>.duckdb"

# Re-run the failed DAG task
airflow tasks run psx_pipeline validate_manifest <execution_date>
```

### 3.3 API Returning Stale Data

**Symptom:** `/analytics/{symbol}` returns rows with `session_date` > 5 trading days old.

**Check:**

```bash
# Verify last successful DAG run
airflow dags list-runs -d psx_pipeline --state success --limit 1

# Check fact row count for symbol
python3 -c "
import duckdb, os
conn = duckdb.connect(os.environ['PSX_DATA_ROOT'] + '/psx_analytics.duckdb', read_only=True)
print(conn.execute(\"SELECT MAX(session_date) FROM fact_daily_analytics\").fetchone())
"
```

---

## 4. Backup and Restore

```bash
# Create backup
./scripts/backup.sh

# Dry-run restore (verify checksum without overwriting)
./scripts/restore.sh /path/to/backup.duckdb --dry-run

# Actual restore
./scripts/restore.sh /path/to/backup.duckdb
```

Backups are retained for `RETENTION_DAYS=30` (configurable). Backup directory: `${PSX_DATA_ROOT}/backups/`.

---

## 5. Escalation

For issues not resolved by this runbook, file a GitHub issue using the bug report template at `.github/ISSUE_TEMPLATE/bug_report.md`. Security vulnerabilities go to `raldisk@heraldcollamar.com` directly — do not open a public issue.
