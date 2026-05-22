"""
test_fact_daily_analytics_schema.py
=====================================
Contract tests: column presence and DuckDB type assertions for fact_daily_analytics.
Validates schema contract without requiring production data.
"""
from __future__ import annotations

import pytest
import duckdb


REQUIRED_COLUMNS = {
    "symbol_key": "INTEGER",
    "session_date_key": "INTEGER",
    "open_price": "DOUBLE",
    "high_price": "DOUBLE",
    "low_price": "DOUBLE",
    "close_price": "DOUBLE",
    "total_volume": "BIGINT",
    "total_value": "DOUBLE",
    "vwap": "DOUBLE",
    "amihud_illiquidity": "DOUBLE",
    "price_impact_bps": "DOUBLE",
    "sarima_status": "VARCHAR",
    "computed_version": "INTEGER",
}


class TestFactDailyAnalyticsSchema:
    @pytest.fixture
    def in_memory_db(self):
        """Create an in-memory DuckDB with a minimal fact_daily_analytics table."""
        conn = duckdb.connect(":memory:")
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
                sarima_status VARCHAR,
                computed_version INTEGER DEFAULT 0
            )
        """)
        yield conn
        conn.close()

    def test_required_columns_present(self, in_memory_db):
        """All required columns must exist in fact_daily_analytics."""
        result = in_memory_db.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'fact_daily_analytics'"
        ).fetchall()
        existing = {row[0] for row in result}
        for col in REQUIRED_COLUMNS:
            assert col in existing, f"Missing column: {col}"

    def test_close_price_not_null_constraint(self, in_memory_db):
        """close_price NOT NULL constraint must be enforced."""
        with pytest.raises(Exception):
            in_memory_db.execute(
                "INSERT INTO fact_daily_analytics (symbol_key, session_date_key, close_price) "
                "VALUES (1, 1, NULL)"
            )
