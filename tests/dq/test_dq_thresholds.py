"""
test_dq_thresholds.py
=====================
Data quality threshold tests: close_price > 0, volume >= 0, no future dates.
"""
from __future__ import annotations

import pytest
import duckdb
from datetime import date


@pytest.fixture
def dq_db():
    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE fact_daily_analytics (
            symbol_key INTEGER,
            session_date DATE,
            close_price DOUBLE,
            total_volume BIGINT
        )
    """)
    # Seed with valid data
    conn.execute("""
        INSERT INTO fact_daily_analytics VALUES
        (1, '2024-01-02', 150.50, 1000000),
        (1, '2024-01-03', 151.25, 850000),
        (2, '2024-01-02', 75.00, 500000)
    """)
    yield conn
    conn.close()


class TestDQThresholds:
    def test_close_price_always_positive(self, dq_db):
        """close_price must be > 0 for all rows."""
        result = dq_db.execute(
            "SELECT COUNT(*) FROM fact_daily_analytics WHERE close_price <= 0"
        ).fetchone()[0]
        assert result == 0, f"{result} rows with close_price <= 0"

    def test_volume_non_negative(self, dq_db):
        """total_volume must be >= 0."""
        result = dq_db.execute(
            "SELECT COUNT(*) FROM fact_daily_analytics WHERE total_volume < 0"
        ).fetchone()[0]
        assert result == 0, f"{result} rows with negative volume"

    def test_no_future_session_dates(self, dq_db):
        """session_date must not exceed current_date."""
        result = dq_db.execute(
            "SELECT COUNT(*) FROM fact_daily_analytics WHERE session_date > current_date"
        ).fetchone()[0]
        assert result == 0, f"{result} rows with future session_date"

    def test_dq_violation_detected(self, dq_db):
        """DQ tests must actually catch violations — self-validating."""
        dq_db.execute(
            "INSERT INTO fact_daily_analytics VALUES (3, '2024-01-04', -1.0, 100)"
        )
        result = dq_db.execute(
            "SELECT COUNT(*) FROM fact_daily_analytics WHERE close_price <= 0"
        ).fetchone()[0]
        assert result == 1, "DQ test failed to detect injected violation"
