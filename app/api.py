"""
api.py
======
FastAPI serving layer for P3 PSX Analytics.

F-022 enforcement: every endpoint that queries fact_trade or fact_daily_analytics
MUST call validate_date_range() before opening a DuckDB connection. This is not
optional — it is the only mechanism preventing full-table scans from exhausting
DuckDB memory and crashing the serving layer for all concurrent sessions.

GSR-005 enforcement: all serving connections use read_only=True via
serving_connection(). The pipeline's write lock is never contested at query time.

Architecture boundary:
  FastAPI (this file) → duckdb_manager.serving_connection() → DuckDB (read_only)
  Pipeline DAG        → duckdb_manager.pipeline_connection() → DuckDB (read_write)
  These two paths NEVER run concurrently by design.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

log = logging.getLogger(__name__)

app = FastAPI(
    title="PSX Analytics API",
    description=(
        "P3 PSX Equity Market Microstructure Analytics — read-only serving layer. "
        "All fact-table endpoints require start_date and end_date parameters. "
        "Maximum date range: PSX_MAX_DATE_RANGE_DAYS (default: 90 days). "
        "DuckDB memory limit: PSX_DUCKDB_MEMORY_LIMIT (default: 2GB)."
    ),
    version="1.0.0",
)


# ─── Dependency: deferred import to allow env var override before startup ──────

def _get_duckdb_manager():
    from duckdb_manager import serving_connection, validate_date_range
    return serving_connection, validate_date_range


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    """Liveness check — does not open DuckDB connection."""
    return {"status": "ok", "service": "psx-analytics"}


@app.get("/readiness")
def readiness():
    """Readiness check — verifies DuckDB file is accessible."""
    from duckdb_manager import DUCKDB_PATH
    serving_conn, _ = _get_duckdb_manager()
    if not DUCKDB_PATH.exists():
        raise HTTPException(status_code=503, detail=f"DuckDB not found: {DUCKDB_PATH}")
    try:
        with serving_conn() as conn:
            result = conn.execute("SELECT COUNT(*) FROM dim_symbol").fetchone()
        return {"status": "ready", "symbol_count": result[0]}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DuckDB readiness failed: {e}")


# ─── Daily analytics ──────────────────────────────────────────────────────────

@app.get("/analytics/daily")
def get_daily_analytics(
    symbol: str = Query(..., description="PSX stock symbol, e.g. 'ALI'"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD (required)"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD (required)"),
    computed_version: Optional[int] = Query(None, description="Specific computed/ version for historical replay"),
):
    """
    Daily OHLCV + analytics for a symbol over a date range.

    Non-additive measures (VWAP, Amihud illiquidity, price_impact_bps) are at
    daily grain — semantically correct per F-023 fix.

    SARIMA trend_component may be NULL if SARIMA failed to converge for this
    symbol (sarima_status='FAILED_CONVERGENCE'). Use sarima_status to filter.

    F-022: start_date and end_date are REQUIRED. Missing or out-of-range dates
    raise HTTP 422 before any DuckDB query is attempted.
    """
    serving_conn, validate = _get_duckdb_manager()
    try:
        validate(start_date, end_date, table_name="fact_daily_analytics")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    # Version filter: default = current computed version (computed_version IS NULL treated as 0)
    version_clause = (
        f"AND f.computed_version = {computed_version}"
        if computed_version is not None
        else "AND f.computed_version = (SELECT MAX(computed_version) FROM fact_daily_analytics WHERE symbol_key = s.symbol_key)"
    )

    sql = f"""
        SELECT
            s.symbol_code,
            d.session_date,
            f.open_price,
            f.high_price,
            f.low_price,
            f.close_price,
            f.total_volume,
            f.total_value,
            f.vwap,
            f.amihud_illiquidity,
            f.price_impact_bps,
            f.trend_component,
            f.sarima_status,
            f.computed_version
        FROM fact_daily_analytics f
        JOIN dim_symbol  s ON s.symbol_key   = f.symbol_key
        JOIN dim_session d ON d.session_date_key = f.session_date_key
        WHERE s.symbol_code = ?
          AND d.session_date BETWEEN ? AND ?
          {version_clause}
        ORDER BY d.session_date
    """

    try:
        with serving_conn() as conn:
            rows = conn.execute(sql, [symbol, start_date, end_date]).fetchall()
            cols = [d[0] for d in conn.description]
    except Exception as e:
        log.error("DuckDB query failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Query error: {e}")

    return {
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
        "computed_version": computed_version,
        "row_count": len(rows),
        "data": [dict(zip(cols, row)) for row in rows],
        "_note": (
            "vwap, amihud_illiquidity, price_impact_bps are daily aggregates — "
            "do not SUM these across rows. Use total_volume and total_value for additive aggregations."
        ),
    }


@app.get("/analytics/tick")
def get_tick_data(
    symbol: str = Query(...),
    start_date: Optional[str] = Query(None, description="Required. YYYY-MM-DD."),
    end_date: Optional[str] = Query(None, description="Required. YYYY-MM-DD."),
):
    """
    Raw tick data for a symbol.
    Contains ONLY additive measures: price, volume, value.
    Non-additive analytics (VWAP, etc.) are in /analytics/daily.

    F-022: start_date and end_date REQUIRED.
    """
    serving_conn, validate = _get_duckdb_manager()
    try:
        validate(start_date, end_date, table_name="fact_trade")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    sql = """
        SELECT
            s.symbol_code,
            f.trade_timestamp_ms,
            f.price,
            f.volume,
            f.value,
            f._ingested_at
        FROM fact_trade f
        JOIN dim_symbol  s ON s.symbol_key = f.symbol_key
        JOIN dim_session d ON d.session_date_key = f.session_key
        WHERE s.symbol_code = ?
          AND d.session_date BETWEEN ? AND ?
        ORDER BY f.trade_timestamp_ms
    """

    try:
        with serving_conn() as conn:
            rows = conn.execute(sql, [symbol, start_date, end_date]).fetchall()
            cols = [d[0] for d in conn.description]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {e}")

    return {
        "symbol": symbol,
        "start_date": start_date,
        "end_date": end_date,
        "row_count": len(rows),
        "data": [dict(zip(cols, row)) for row in rows],
    }


@app.get("/analytics/sarima-status")
def get_sarima_status(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """
    Returns SARIMA convergence status for all symbols in date range.
    Use to identify which symbols have NULL trend_component due to F-025 isolation.
    """
    serving_conn, validate = _get_duckdb_manager()
    try:
        validate(start_date, end_date, table_name="fact_daily_analytics")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))

    sql = """
        SELECT
            s.symbol_code,
            f.sarima_status,
            COUNT(*) AS trading_days,
            SUM(CASE WHEN f.trend_component IS NULL THEN 1 ELSE 0 END) AS null_trend_days
        FROM fact_daily_analytics f
        JOIN dim_symbol  s ON s.symbol_key = f.symbol_key
        JOIN dim_session d ON d.session_date_key = f.session_date_key
        WHERE d.session_date BETWEEN ? AND ?
        GROUP BY s.symbol_code, f.sarima_status
        ORDER BY s.symbol_code
    """

    with serving_conn() as conn:
        rows = conn.execute(sql, [start_date, end_date]).fetchall()
        cols = [d[0] for d in conn.description]

    return {"data": [dict(zip(cols, row)) for row in rows]}


@app.get("/manifest/canonical")
def get_canonical_path(symbol: str = Query(...), session_date: str = Query(...)):
    """
    Returns the manifest-canonical Parquet path for (symbol, session_date).
    Operational: confirms which file is authoritative after an amendment.
    """
    from psx_ingest import get_canonical_computed_path, load_manifest, manifest_key
    from duckdb_manager import DUCKDB_PATH

    manifest_pth = DUCKDB_PATH.parent / "manifest.json"
    key = manifest_key(symbol, session_date)
    manifest = load_manifest(manifest_pth)
    entry = manifest.get(key)

    if not entry:
        raise HTTPException(status_code=404, detail=f"No manifest entry for {key}")

    return {
        "key": key,
        "canonical_raw_path": entry.get("raw_path"),
        "amended": entry.get("amended"),
        "prior_raw_path": entry.get("prior_raw_path"),
        "computed_version": entry.get("computed_version"),
        "row_count": entry.get("row_count"),
        "ingested_at": entry.get("ingested_at"),
    }