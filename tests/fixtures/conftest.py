"""
conftest.py
===========
Shared pytest fixtures for P3 PSX Analytics test suite.
Provides a tmp DuckDB instance pre-seeded with 90-day synthetic OHLCV data for 5 PSX symbols.
"""
from __future__ import annotations

import json
import pytest
import duckdb
from pathlib import Path
from datetime import date, timedelta

SYMBOLS = ["SM", "ALI", "BDO", "TEL", "JFC"]
BASE_DATE = date(2024, 1, 2)
TRADING_DAYS = 90


def _generate_ohlcv(symbol: str, session_date: date):
    """Generate deterministic synthetic OHLCV for a given symbol and date."""
    seed = hash(f"{symbol}{session_date}") % 1000
    base_price = 100.0 + (seed % 200)
    return {
        "symbol": symbol,
        "session_date": session_date.isoformat(),
        "open_price": round(base_price, 2),
        "high_price": round(base_price * 1.02, 2),
        "low_price": round(base_price * 0.98, 2),
        "close_price": round(base_price * 1.001, 2),
        "total_volume": 100000 + (seed * 1000),
        "total_value": round((base_price * 1.001) * (100000 + seed * 1000), 2),
    }


@pytest.fixture(scope="session")
def seeded_duckdb(tmp_path_factory):
    """
    Session-scoped DuckDB fixture with 90-day synthetic OHLCV for 5 PSX symbols.
    Seeded with dim_symbol, dim_session, and fact_daily_analytics tables.
    """
    tmp_path = tmp_path_factory.mktemp("duckdb")
    db_file = tmp_path / "test_psx.duckdb"

    conn = duckdb.connect(str(db_file))

    conn.execute("""
        CREATE TABLE dim_symbol (
            symbol_key INTEGER PRIMARY KEY,
            symbol_code VARCHAR NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE dim_session (
            session_date_key INTEGER PRIMARY KEY,
            session_date DATE NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE fact_daily_analytics (
            symbol_key INTEGER NOT NULL,
            session_date_key INTEGER NOT NULL,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE NOT NULL,
            total_volume BIGINT,
            total_value DOUBLE,
            vwap DOUBLE,
            amihud_illiquidity DOUBLE,
            price_impact_bps DOUBLE,
            trend_component DOUBLE,
            sarima_status VARCHAR DEFAULT 'CONVERGED',
            computed_version INTEGER DEFAULT 0
        )
    """)

    # Seed dim_symbol
    for i, sym in enumerate(SYMBOLS, start=1):
        conn.execute("INSERT INTO dim_symbol VALUES (?, ?)", [i, sym])

    # Seed dim_session and fact_daily_analytics
    current = BASE_DATE
    day_key = 1
    for _ in range(TRADING_DAYS):
        conn.execute("INSERT INTO dim_session VALUES (?, ?)", [day_key, current.isoformat()])
        for sym_key, sym in enumerate(SYMBOLS, start=1):
            row = _generate_ohlcv(sym, current)
            vwap = row["total_value"] / row["total_volume"]
            conn.execute("""
                INSERT INTO fact_daily_analytics VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 'CONVERGED', 0
                )
            """, [
                sym_key, day_key,
                row["open_price"], row["high_price"], row["low_price"], row["close_price"],
                row["total_volume"], row["total_value"],
                round(vwap, 4), round(0.0001 / vwap, 8), round(vwap * 0.0001, 4),
            ])
        current += timedelta(days=1)
        day_key += 1

    conn.close()
    yield db_file
    # tmp_path_factory handles cleanup


@pytest.fixture(scope="session")
def seed_manifest(tmp_path_factory):
    """Session-scoped manifest fixture matching the seeded DuckDB parquet paths."""
    tmp_path = tmp_path_factory.mktemp("manifest")
    manifest = {}
    current = BASE_DATE
    for _ in range(TRADING_DAYS):
        for sym in SYMBOLS:
            key = f"{sym}_{current.isoformat()}"
            manifest[key] = {
                "symbol": sym,
                "session_date": current.isoformat(),
                "canonical_raw_path": f"/data/raw/{sym}_{current.strftime('%Y%m%d')}.parquet",
                "row_count": 100,
                "amended": False,
            }
        current += timedelta(days=1)

    manifest_file = tmp_path / "seed_manifest.json"
    manifest_file.write_text(json.dumps(manifest, indent=2))
    yield manifest_file
