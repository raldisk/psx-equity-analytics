# Known Issues — PSX Analytics (P3)

## KI-001 · File-Swap Defect in `.env.example` (repaired in Phase 3)

**Symptom:** The committed `.env.example` contained verbatim `.gitignore` content — a commit-time file swap. Any developer copying `.env.example` to `.env` would configure their environment from gitignore patterns rather than actual environment variables.

**Status:** ✅ Repaired in Phase 3 (P3-PROP-01). `.env.example` now contains the correct PSX-specific variable template. The `.gitignore` was separately present and was not affected.

**Root cause:** Accidental file swap at commit time.

---

## KI-002 · Dual Serving Layer — `app/api.py` vs `serving/psx_analytics_api.py` (resolved Phase 3)

**Symptom:** Two near-identical FastAPI entry points existed: `app/api.py` and `serving/psx_analytics_api.py`. Both declared the same architecture boundary constraints and identical import patterns. Any reference to `app/api.py` in documentation or tooling pointed to a file that could diverge from the canonical serving layer.

**Status:** ✅ Resolved in Phase 3 (P3-PROP-02). `app/api.py` and the `app/` directory were deleted. The canonical entry point is `serving/psx_analytics_api.py`.

---

## KI-003 · DuckDB WAL File Orphan on SIGKILL

**Symptom:** If the pipeline process receives `SIGKILL` (e.g., OOM kill, `kill -9`) while holding the DuckDB write lock, a `.wal` file is left at `psx_analytics.duckdb.wal`. On next open, DuckDB replays the WAL and recovers — but if the WAL is corrupt, the next open will fail with a checkpoint error.

**Workaround:** The Prometheus DuckDB exporter monitors WAL presence (`duckdb_wal_present` metric). A Grafana alert on this metric > 0 for >5 minutes signals a potential stuck lock. Recovery: stop all processes touching the `.duckdb` file, delete the `.wal`, and restart.

**Permanent fix:** Use `pipeline_connection()` exclusively for writes and ensure Airflow tasks are gracefully terminated (SIGTERM, not SIGKILL) before task timeout. The `on_failure_callback` in the Airflow DAG can emit a structured log warning when unexpected termination occurs.

**Status:** ⚠️ Open — behavior is inherent to DuckDB's WAL model. Monitoring in place via `prometheus/exporters/duckdb_metrics.py`.

---

## KI-004 · Governance Duplicate Files (resolved Phase 3)

**Symptom:** `governance/` contained both prefixed (`P3-CLOSURE-DECLARATION.md`, `P3-HARDENING-LOG.md`) and unprefixed (`closure-declaration.md`, `hardening-log.md`) versions of the same documents.

**Status:** ✅ Resolved in Phase 3 (P3-PROP-03). Unprefixed duplicates deleted. Only P3-prefixed canonical files remain.

---

## KI-005 · SARIMA Convergence Rate on Thin-Volume Symbols

**Symptom:** PSX symbols with fewer than ~30 trading days of data in the trailing 90-day window produce SARIMA `NON_CONVERGENT` status rather than `CONVERGED`. This is expected statistical behavior, not a bug, but causes the Grafana "SARIMA success rate" panel to read low for newly-listed or illiquid symbols.

**Workaround:** Filter `sarima_status = 'CONVERGED'` in API consumers when computing trend components. The pipeline emits a structured log warning (`sarima_status=NON_CONVERGENT symbol=<sym>`) for monitoring.

**Status:** ⚠️ Open — by design. Document in consumer runbooks.
