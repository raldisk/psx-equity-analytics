"""
psx_pipeline_dag.py
===================
P3 PSX Analytics — pipeline orchestration DAG.

Architecture notes:
  - No Airflow Connections used. DuckDB is file-local; no DB hook needed.
  - No Kafka, no Debezium, no streaming. Batch only — PSX EOD CSV drop.
  - GSR-005: Pipeline runs during off-hours window (02:00–06:00 PHT) when
    serving traffic is minimal. This is not enforced by code — it is an
    operational scheduling convention.
  - Stateless: every task reads from disk and writes to disk. Re-running any
    task from scratch is safe and produces identical output (deterministic).

Task dependency chain:
  detect_psx_csv_drop
      ↓
  validate_and_ingest_csv        (F-019: manifest-based; F-021: manifest authority)
      ↓
  initialize_duckdb_schema       (idempotent: CREATE TABLE IF NOT EXISTS)
      ↓
  dbt_staging_run                (reads from manifest-path source, not glob)
      ↓
  settlement_enrichment          (Edge D — optional R5 consumer; always succeeds)
      ↓
  dbt_marts_run                  (fact_daily_analytics: F-023 grain, F-025 SARIMA isolation)
      ↓
  dq_assertions                  (DQ gate — completeness and range checks)
      ↓
  update_serving_metadata        (marks pipeline run complete in manifest)

Edge D integration notes:
  settlement_enrichment runs between dbt_staging and dbt_marts. It fetches
  daily interbank settlement PHP flow from iso20022-settlement-engine (R5) and
  writes data/enrichment/settlement_{date}.parquet. On any failure (R5 absent,
  timeout, bad response) it writes an empty Parquet and succeeds — pipeline is
  never blocked. dbt_marts uses trigger_rule='all_done' so it runs even in the
  unexpected event settlement_enrichment raises at the Python level.

DQ gate behavior:
  dbt test failures log WARNING and do NOT block the pipeline by default.
  Set PSX_DQ_HARD_FAIL=true to block on test failure.
  Rationale: PSX analytics is a research/portfolio artifact, not a regulatory
  submission. Blocking on every DQ warning would impede fund manager access
  to market data. Critical failures (price=0, row_count=0) DO always block.
"""

from __future__ import annotations

import logging
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# ── Settlement enrichment (Edge D — optional R5 consumer) ────────────────────
# Import is deferred to inside the callable to match existing DAG pattern.
# SETTLEMENT_API_URL absent → fetch_and_write writes empty Parquet, task succeeds.
from datetime import date as _date

log = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────

PSX_DROP_DIR    = Path(os.getenv("PSX_DROP_DIR",    "/opt/airflow/data/psx_drops"))
PSX_DATA_ROOT   = Path(os.getenv("PSX_DATA_ROOT",   "/opt/airflow/data"))
DBT_PROJECT_DIR = Path(os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt"))
DBT_PROFILES_DIR= Path(os.getenv("DBT_PROFILES_DIR","/opt/airflow/dbt/profiles"))
DBT_TARGET      = os.getenv("DBT_TARGET", "prod")
DQ_HARD_FAIL    = os.getenv("PSX_DQ_HARD_FAIL", "false").lower() == "true"

MANIFEST_PATH   = PSX_DATA_ROOT / "manifest.json"
DUCKDB_PATH     = PSX_DATA_ROOT / "psx_analytics.duckdb"

# ─── Task implementations ─────────────────────────────────────────────────────

def detect_psx_csv_drop(**context) -> None:
    """
    Scans PSX_DROP_DIR for new EOD CSV files.
    Expects files named: {SYMBOL}_{YYYYMMDD}.csv or PSX_EOD_{YYYYMMDD}.csv.
    Pushes list of (csv_path, symbol, session_date) tuples to XCom.

    If no new files are found: raises AirflowSkipException.
    The DAG marks all downstream tasks as SKIPPED — correct behavior for
    market holidays or delayed drops.
    """
    PSX_DROP_DIR.mkdir(parents=True, exist_ok=True)

    import re
    csv_files = list(PSX_DROP_DIR.glob("*.csv"))
    if not csv_files:
        raise AirflowSkipException(
            f"No CSV files found in {PSX_DROP_DIR}. "
            f"Market holiday or delayed drop — DAG skipped."
        )

    detected = []
    for csv_path in sorted(csv_files):
        # Try to extract symbol and date from filename
        m = re.match(r"^(?P<symbol>[A-Z0-9.]+)_(?P<date>\d{8})\.csv$", csv_path.name)
        if m:
            symbol       = m.group("symbol")
            session_date = f"{m.group('date')[:4]}-{m.group('date')[4:6]}-{m.group('date')[6:]}"
        else:
            # Fallback: use filename stem as symbol, today as session_date
            session_date = context["execution_date"].in_timezone("Asia/Manila").format("YYYY-MM-DD")
            symbol       = csv_path.stem.upper()

        detected.append({"csv_path": str(csv_path), "symbol": symbol, "session_date": session_date})
        log.info("Detected: %s → symbol=%s date=%s", csv_path.name, symbol, session_date)

    context["ti"].xcom_push(key="detected_files", value=detected)
    log.info("Total PSX CSV files detected: %d", len(detected))


def validate_and_ingest_csv(**context) -> None:
    """
    F-019+F-021+F-024: Ingest each detected CSV via psx_ingest.ingest_psx_csv().
    The manifest controls canonical path per (symbol, date).
    Amendment detection is automatic — prior file retained; manifest updated.
    """
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
    from psx_ingest import ingest_psx_csv

    detected = context["ti"].xcom_pull(task_ids="detect_psx_csv_drop", key="detected_files")
    if not detected:
        raise AirflowSkipException("No files to ingest.")

    raw_dir = PSX_DATA_ROOT / "raw"
    ingested = []
    errors   = []

    for item in detected:
        csv_path    = Path(item["csv_path"])
        symbol      = item["symbol"]
        session_date = item["session_date"]
        try:
            entry = ingest_psx_csv(csv_path, symbol, session_date, raw_dir, MANIFEST_PATH)
            ingested.append({"symbol": symbol, "session_date": session_date,
                             "row_count": entry["row_count"], "amended": entry["amended"]})
            log.info("Ingested: %s/%s rows=%d amended=%s",
                     symbol, session_date, entry["row_count"], entry["amended"])
        except FileExistsError as e:
            log.info("Skipping (already canonical): %s/%s — %s", symbol, session_date, e)
        except ValueError as e:
            log.error("Validation failed: %s/%s — %s", symbol, session_date, e)
            errors.append({"symbol": symbol, "session_date": session_date, "error": str(e)})
        except Exception as e:
            log.error("Unexpected ingest error: %s/%s — %s", symbol, session_date, e)
            errors.append({"symbol": symbol, "session_date": session_date, "error": str(e)})

    context["ti"].xcom_push(key="ingested_files", value=ingested)
    context["ti"].xcom_push(key="ingest_errors",  value=errors)

    if errors:
        raise AirflowException(
            f"Ingest completed with {len(errors)} error(s): {errors}. "
            f"Successfully ingested: {len(ingested)} file(s). "
            f"Review errors before proceeding."
        )
    log.info("Ingest complete: %d files ingested, 0 errors", len(ingested))


def initialize_duckdb_schema(**context) -> None:
    """Idempotent: CREATE TABLE IF NOT EXISTS for all P3 tables."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
    from duckdb_manager import initialize_schema
    initialize_schema(DUCKDB_PATH)
    log.info("DuckDB schema initialized: %s", DUCKDB_PATH)


def run_dbt(select: str, **context) -> None:
    """
    Run dbt with the given --select target.
    F-025: dbt Python model handles per-symbol SARIMA isolation internally.
    This task succeeds even if some symbols have SARIMA convergence failures —
    those symbols get sarima_status=FAILED_CONVERGENCE in the mart.
    """
    cmd = [
        "dbt", "run",
        "--profiles-dir", str(DBT_PROFILES_DIR),
        "--project-dir",  str(DBT_PROJECT_DIR),
        "--target",       DBT_TARGET,
        "--select",       select,
    ]
    log.info("Running: %s", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(DBT_PROJECT_DIR))
    log.info("dbt stdout:\n%s", proc.stdout[-2000:])
    if proc.returncode != 0:
        raise AirflowException(f"dbt run failed (exit {proc.returncode}):\n{proc.stderr[-1000:]}")


def run_settlement_enrichment(**context) -> None:
    """
    Optional pre-step: fetch bilateral settlement flow from R5.

    Design contract:
      - Always succeeds (non-fatal). Empty Parquet written on any failure.
      - Airflow task failure therefore signals a programming error, not an R5 outage.
      - SETTLEMENT_API_URL absent → enrichment silently disabled.

    Trigger rule on THIS task: default (all_success).
    Trigger rule on downstream dbt_marts: all_done (set on that task).
    This ensures dbt_marts runs whether this task succeeds or is skipped.
    """
    import sys
    from pathlib import Path

    # Add scripts/ to path so dbt env can find settlement_enrichment
    sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))
    from settlement_enrichment import fetch_and_write

    execution_date = context.get("ds")
    target = _date.fromisoformat(execution_date) if execution_date else _date.today()
    fetch_and_write(target)


def run_dq_assertions(**context) -> None:
    """
    Run dbt tests. Behavior controlled by PSX_DQ_HARD_FAIL env var:
      false (default): warnings logged; pipeline continues
      true:            any test failure raises AirflowException

    Critical failures (price=0, row_count=0) always block regardless of flag.
    Rationale: PSX analytics is a research artifact, not a regulatory submission.
    """
    cmd = [
        "dbt", "test",
        "--profiles-dir", str(DBT_PROFILES_DIR),
        "--project-dir",  str(DBT_PROJECT_DIR),
        "--target",       DBT_TARGET,
        "--store-failures",
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, cwd=str(DBT_PROJECT_DIR))
    log.info("dbt test stdout:\n%s", proc.stdout[-2000:])

    # Parse run_results.json for critical failures
    import json
    results_path = DBT_PROJECT_DIR / "target" / "run_results.json"
    critical_failures = []
    warnings = []

    if results_path.exists():
        with open(results_path) as f:
            run_results = json.load(f)
        for r in run_results.get("results", []):
            if r.get("status") in ("fail", "error"):
                test_id = r.get("unique_id", "")
                # Classify as critical or warning
                if any(tag in test_id for tag in ("not_null_price", "row_count_gt_zero")):
                    critical_failures.append(test_id)
                else:
                    warnings.append(test_id)

    if critical_failures:
        raise AirflowException(
            f"DQ CRITICAL FAILURE: {len(critical_failures)} critical test(s) failed: "
            f"{critical_failures}. Pipeline halted."
        )

    if warnings:
        if DQ_HARD_FAIL:
            raise AirflowException(
                f"DQ HARD FAIL MODE: {len(warnings)} test warning(s) treated as failures: "
                f"{warnings}"
            )
        log.warning(
            "DQ warnings (%d tests): %s. Pipeline continues (PSX_DQ_HARD_FAIL=false).",
            len(warnings), warnings
        )


def update_serving_metadata(**context) -> None:
    """
    Marks the pipeline run as complete in the manifest.
    The serving layer reads this to know the latest available session.
    """
    import json, sys
    sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
    from psx_ingest import load_manifest, save_manifest

    manifest = load_manifest(MANIFEST_PATH)
    manifest["_pipeline_last_run"] = {
        "completed_at":   pendulum.now("Asia/Manila").isoformat(),
        "dag_run_id":     context["run_id"],
        "execution_date": context["execution_date"].isoformat(),
    }
    save_manifest(manifest, MANIFEST_PATH)
    log.info("Serving metadata updated: pipeline run complete")


# ─── DAG ──────────────────────────────────────────────────────────────────────

default_args = {
    "owner":   "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id            = "psx_pipeline_dag",
    description       = "P3: PSX EOD ingest → DuckDB → dbt analytics → DQ gate",
    start_date        = pendulum.datetime(2025, 1, 1, tz="Asia/Manila"),
    schedule_interval = "0 2 * * 1-5",   # 02:00 PHT Mon–Fri (trading days only)
    catchup           = False,
    max_active_runs   = 1,               # Serialized: pipeline holds DuckDB write lock
    default_args      = default_args,
    tags              = ["psx", "analytics", "p3"],
) as dag:

    start = EmptyOperator(task_id="start")

    detect = PythonOperator(
        task_id="detect_psx_csv_drop",
        python_callable=detect_psx_csv_drop,
    )

    ingest = PythonOperator(
        task_id="validate_and_ingest_csv",
        python_callable=validate_and_ingest_csv,
    )

    init_schema = PythonOperator(
        task_id="initialize_duckdb_schema",
        python_callable=initialize_duckdb_schema,
    )

    dbt_staging = PythonOperator(
        task_id="dbt_staging_run",
        python_callable=run_dbt,
        op_kwargs={"select": "staging"},
    )

    # ── Edge D Consumer: optional R5 settlement enrichment ───────────────────
    settlement_enrichment = PythonOperator(
        task_id="settlement_enrichment",
        python_callable=run_settlement_enrichment,
        pool="psx_pool",          # same pool as sibling tasks — respects concurrency limits
        dag=dag,
        doc_md="""
        **Edge D Consumer — Optional R5 settlement enrichment.**

        Fetches daily interbank settlement PHP flow from iso20022-settlement-engine.
        Writes data/enrichment/settlement_{date}.parquet regardless of outcome.
        If SETTLEMENT_API_URL is unset or R5 is unreachable, writes empty Parquet and succeeds.

        The downstream dbt_marts task uses trigger_rule='all_done' so it runs
        even in the (unexpected) event this task fails at the Python level.
        """,
    )

    dbt_marts = PythonOperator(
        task_id="dbt_marts_run",
        python_callable=run_dbt,
        op_kwargs={"select": "marts"},
        trigger_rule="all_done",   # Run even if settlement_enrichment task fails
    )

    dq_gate = PythonOperator(
        task_id="dq_assertions",
        python_callable=run_dq_assertions,
    )

    update_meta = PythonOperator(
        task_id="update_serving_metadata",
        python_callable=update_serving_metadata,
    )

    end = EmptyOperator(task_id="end")

    # Dependency chain — settlement_enrichment inserted between staging and marts
    (
        start
        >> detect
        >> ingest
        >> init_schema
        >> dbt_staging
        >> settlement_enrichment
        >> dbt_marts
        >> dq_gate
        >> update_meta
        >> end
    )
