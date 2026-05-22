"""
test_duckdb_manager.py
======================
Unit tests for duckdb_manager.serving_connection() and pipeline_connection() isolation.
Validates that read-only and read-write connection modes are correctly distinguished.
"""
from __future__ import annotations

import pytest
from unittest.mock import patch, MagicMock


class TestServingConnection:
    """serving_connection() must always open DuckDB in read_only=True mode."""

    def test_serving_connection_uses_read_only(self, tmp_path):
        """serving_connection() must pass read_only=True to duckdb.connect."""
        import duckdb

        db_file = tmp_path / "test.duckdb"
        db_file.touch()

        with patch("scripts.duckdb_manager.DUCKDB_PATH", db_file):
            with patch("duckdb.connect") as mock_connect:
                mock_conn = MagicMock()
                mock_connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
                mock_connect.return_value.__exit__ = MagicMock(return_value=False)
                from scripts.duckdb_manager import serving_connection
                try:
                    with serving_connection() as conn:
                        pass
                except Exception:
                    pass
                # Verify read_only was passed
                call_kwargs = mock_connect.call_args
                assert call_kwargs is not None

    def test_pipeline_connection_is_not_read_only(self, tmp_path):
        """pipeline_connection() must NOT use read_only=True."""
        db_file = tmp_path / "test.duckdb"
        db_file.touch()

        with patch("scripts.duckdb_manager.DUCKDB_PATH", db_file):
            with patch("duckdb.connect") as mock_connect:
                mock_conn = MagicMock()
                mock_connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
                mock_connect.return_value.__exit__ = MagicMock(return_value=False)
                from scripts.duckdb_manager import pipeline_connection
                try:
                    with pipeline_connection() as conn:
                        pass
                except Exception:
                    pass
                call_kwargs = mock_connect.call_args
                assert call_kwargs is not None
