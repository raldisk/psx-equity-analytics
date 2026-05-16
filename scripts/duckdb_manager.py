"""
duckdb_manager.py
=================
DuckDB connection manager for P3 PSX Analytics.

Hardens two adversarial findings:

F-022 (HIGH): DuckDB unbounded memory.
  Without a memory limit, a Streamlit user selecting "all time" for a tick-data
  chart sends a full-table scan that exhausts process memory and crashes the
  serving layer, taking down all concurrent sessions.
  Fix: Every connection enforces a configurable memory_limit at initialization.
  Default: 2GB (Intel Pentium hardware). Production: set via PSX_DUCKDB_MEMORY_LIMIT env.

GSR-005 (MEDIUM): Single-writer lock.
  DuckDB uses a process-level writer lock. A dbt transform DAG and a Streamlit
  query running simultaneously against the same DuckDB file will block each other.
  Fix: Serving connections open the DuckDB file in READ-ONLY mode. Only the
  pipeline (ingest + dbt) connections open in READ-WRITE mode.
  READ-ONLY mode allows unlimited concurrent readers without blocking.
  The pipeline acquires WRITE access only during its scheduled window.

Time semantics:
  All queries must declare their time axis. The query_guard() enforces that
  fact table queries include a date-range predicate. A query without a
  date range is rejected at the API layer before reaching DuckDB.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Optional

import duckdb

log = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────

DUCKDB_PATH = Path(os.getenv("PSX_DUCKDB_PATH", "data/psx_analytics.duckdb"))

# F-022: memory limit — configurable via env; default 2GB for Pentium hardware
MEMORY_LIMIT_DEFAULT = os.getenv("PSX_DUCKDB_MEMORY_LIMIT", "2GB")

# F-022: max date range for tick/fact queries in days
MAX_DATE_RANGE_DAYS  = int(os.getenv("PSX_MAX_DATE_RANGE_DAYS", "90"))

# GSR-005: serving connections are read-only; pipeline connections are read-write
SERVING_READ_ONLY    = os.getenv("PSX_SERVING_READ_ONLY", "true").lower() == "true"


# ─── Connection factory ───────────────────────────────────────────────────────

def _init_connection(conn: duckdb.DuckDBPyConnection, memory_limit: str) -> None:
    """Apply governance settings to a newly opened connection."""
    conn.execute(f"SET memory_limit='{memory_limit}';")
    conn.execute("SET threads=2;")              # Conservative for Pentium hardware
    conn.execute("SET enable_progress_bar=false;")
    conn.execute("PRAGMA enable_object_cache;") # Cache Parquet metadata
    log.debug("DuckDB connection initialized: memory_limit=%s", memory_limit)


@contextmanager
def serving_connection(
    db_path: Path = DUCKDB_PATH,
    memory_limit: str = MEMORY_LIMIT_DEFAULT,
):
    """
    Read-only connection for FastAPI/Streamlit serving layer.

    GSR-005 Fix: read_only=True allows unlimited concurrent readers.
    The write lock is never acquired — pipeline runs can proceed in parallel
    without blocking serving queries (DuckDB WAL handles read isolation).

    F-022 Fix: memory_limit enforced at connection init.
    """
    conn = duckdb.connect(str(db_path), read_only=SERVING_READ_ONLY)
    try:
        _init_connection(conn, memory_limit)
        yield conn
    finally:
        conn.close()


@contextmanager
def pipeline_connection(
    db_path: Path = DUCKDB_PATH,
    memory_limit: str = MEMORY_LIMIT_DEFAULT,
):
    """
    Read-write connection for ingest + dbt pipeline.
    Must not be held open while serving connections are active.
    Pipeline DAG acquires this during its scheduled batch window (02:00–06:00 PHT).
    """
    conn = duckdb.connect(str(db_path), read_only=False)
    try:
        _init_connection(conn, memory_limit)
        yield conn
    finally:
        conn.close()


# ─── Query guard (F-022) ──────────────────────────────────────────────────────

def validate_date_range(
    start_date: Optional[str],
    end_date: Optional[str],
    table_name: str = "fact_trade_daily",
) -> None:
    """
    F-022 Fix: Enforce mandatory date-range predicates on fact table queries.

    Called by FastAPI endpoint handlers before any DuckDB query execution.
    Raises ValueError with a descriptive message if:
      - start_date or end_date is None/empty
      - date range exceeds MAX_DATE_RANGE_DAYS
      - date format is invalid

    This prevents full-table scans from exhausting DuckDB memory.
    An operator CAN override MAX_DATE_RANGE_DAYS via env var for admin queries.
    """
    from datetime import date as date_type
    import re

    _DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

    if not start_date:
        raise ValueError(
            f"Missing required parameter: start_date. "
            f"All queries against {table_name} must specify a date range. "
            f"Without a date range, DuckDB may scan the full tick history "
            f"and exhaust process memory. "
            f"Maximum allowed range: {MAX_DATE_RANGE_DAYS} days."
        )
    if not end_date:
        raise ValueError(
            f"Missing required parameter: end_date. "
            f"All queries against {table_name} must specify a date range."
        )
    if not _DATE_PATTERN.match(start_date) or not _DATE_PATTERN.match(end_date):
        raise ValueError(
            f"Invalid date format. Use YYYY-MM-DD. Got: start={start_date!r} end={end_date!r}"
        )

    from datetime import datetime
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end   = datetime.strptime(end_date,   "%Y-%m-%d").date()

    if end < start:
        raise ValueError(
            f"end_date ({end_date}) must be >= start_date ({start_date})"
        )

    range_days = (end - start).days
    if range_days > MAX_DATE_RANGE_DAYS:
        raise ValueError(
            f"Date range {range_days} days exceeds maximum allowed "
            f"{MAX_DATE_RANGE_DAYS} days. "
            f"Use a narrower date range or set PSX_MAX_DATE_RANGE_DAYS env var "
            f"for administrative queries."
        )


# ─── Schema initialization ────────────────────────────────────────────────────

SCHEMA_SQL = """
-- F-023 Fix: VWAP/Amihud/price_impact removed from raw tick grain.
-- Two separate tables: raw tick grain (additive) and daily analytics grain (computed).

-- Table 1: Raw tick grain — additive measures only
CREATE TABLE IF NOT EXISTS fact_trade (
    symbol_key          INTEGER     NOT NULL,
    trade_timestamp_ms  BIGINT      NOT NULL,  -- Unix timestamp in milliseconds
    session_key         INTEGER     NOT NULL,
    -- Additive measures at tick grain
    price               DECIMAL(12,4) NOT NULL,
    volume              BIGINT      NOT NULL,
    value               DECIMAL(18,2) NOT NULL,
    -- Metadata
    source_file         VARCHAR,
    _ingested_at        TIMESTAMPTZ,
    PRIMARY KEY (symbol_key, trade_timestamp_ms)
);

-- Table 2: Daily analytics grain — non-additive measures computed at correct grain
-- F-023 Fix: VWAP, Amihud illiquidity, price impact are computed DAILY.
-- These are non-additive across ticks. Storing them at tick grain produced
-- silent wrong answers on any SUM() aggregation.
CREATE TABLE IF NOT EXISTS fact_daily_analytics (
    symbol_key          INTEGER     NOT NULL,
    session_date_key    INTEGER     NOT NULL,  -- YYYYMMDD integer
    -- Additive daily measures
    open_price          DECIMAL(12,4),
    high_price          DECIMAL(12,4),
    low_price           DECIMAL(12,4),
    close_price         DECIMAL(12,4),
    total_volume        BIGINT,
    total_value         DECIMAL(18,2),
    trade_count         INTEGER,
    -- Non-additive analytics (daily grain — semantically correct)
    -- VWAP = total_value / total_volume for the session
    vwap                DECIMAL(12,4),
    -- Amihud illiquidity = |daily_return| / daily_volume
    amihud_illiquidity  DECIMAL(18,8),
    -- Price impact bps = bid-ask spread proxy (requires FIX feed for full accuracy)
    price_impact_bps    DECIMAL(10,4),
    -- Trend decomposition (from SARIMA dbt model — may be NULL if SARIMA fails)
    trend_component     DECIMAL(12,4),
    seasonal_component  DECIMAL(12,4),
    sarima_status       VARCHAR DEFAULT 'PENDING',  -- PENDING|OK|FAILED_CONVERGENCE
    -- Computed version for corporate action reproducibility (F-024)
    computed_version    INTEGER NOT NULL DEFAULT 0,
    _computed_at        TIMESTAMPTZ,
    PRIMARY KEY (symbol_key, session_date_key)
);

-- Dimension tables
CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol_key      INTEGER PRIMARY KEY,
    symbol_code     VARCHAR NOT NULL UNIQUE,
    company_name    VARCHAR,
    sector          VARCHAR,
    board           VARCHAR,  -- MAIN, SME, ETF
    listing_date    DATE,
    is_active       BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_session (
    session_key     INTEGER PRIMARY KEY,
    session_date    DATE    NOT NULL UNIQUE,
    trading_day     INTEGER,            -- Calendar day index
    is_trading_day  BOOLEAN DEFAULT TRUE,
    market          VARCHAR DEFAULT 'PSX'
);

-- Corporate action log (F-024: versioning anchor)
CREATE TABLE IF NOT EXISTS corporate_action_log (
    action_id       BIGINT  PRIMARY KEY,
    symbol_key      INTEGER NOT NULL REFERENCES dim_symbol(symbol_key),
    ex_date         DATE    NOT NULL,
    action_type     VARCHAR NOT NULL,   -- SPLIT, RIGHTS, DIVIDEND_CASH, DIVIDEND_STOCK
    adjustment_factor DECIMAL(10,6),    -- price multiplier for backward adjustment
    notes           VARCHAR,
    logged_at       TIMESTAMPTZ DEFAULT current_timestamp,
    triggered_recompute BOOLEAN DEFAULT FALSE
);
"""


def initialize_schema(db_path: Path = DUCKDB_PATH) -> None:
    """Create all tables if they don't exist. Safe to call on every pipeline start."""
    with pipeline_connection(db_path) as conn:
        conn.execute(SCHEMA_SQL)
        log.info("DuckDB schema initialized at %s", db_path)