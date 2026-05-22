"""
test_duckdb_connectivity.py
===========================
Integration test: DuckDB file open/lock round-trip.
Requires no external services — uses a temp DuckDB file.
"""
from __future__ import annotations

import pytest
import duckdb
from pathlib import Path


class TestDuckDBConnectivity:
    def test_duckdb_opens_and_queries(self, tmp_path):
        """DuckDB must open, accept a query, and close cleanly."""
        db_file = tmp_path / "test.duckdb"
        conn = duckdb.connect(str(db_file))
        result = conn.execute("SELECT 42 AS answer").fetchone()
        conn.close()
        assert result[0] == 42

    def test_duckdb_read_only_mode(self, tmp_path):
        """Read-only connection must not allow writes."""
        db_file = tmp_path / "test_ro.duckdb"
        # Create DB first
        conn = duckdb.connect(str(db_file))
        conn.execute("CREATE TABLE t (x INT)")
        conn.close()

        # Open read-only
        ro_conn = duckdb.connect(str(db_file), read_only=True)
        with pytest.raises(Exception):
            ro_conn.execute("INSERT INTO t VALUES (1)")
        ro_conn.close()

    def test_duckdb_wal_cleanup(self, tmp_path):
        """After normal close, no orphaned WAL file should remain."""
        db_file = tmp_path / "test_wal.duckdb"
        conn = duckdb.connect(str(db_file))
        conn.execute("CREATE TABLE t (x INT)")
        conn.execute("INSERT INTO t VALUES (1)")
        conn.close()
        wal_file = Path(str(db_file) + ".wal")
        assert not wal_file.exists(), "WAL file should be cleaned up on normal close"
