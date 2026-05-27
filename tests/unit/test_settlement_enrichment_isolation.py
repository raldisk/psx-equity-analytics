"""
Isolation tests for settlement_enrichment.py.
Fitness function: verifies Edge D can be cleanly disabled and never blocks R4.

These tests MUST pass without R5 running and without SETTLEMENT_API_URL set.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "scripts"))


class TestSettlementEnrichmentIsolation:
    """Fitness function: R4 operates fully without SETTLEMENT_API_URL."""

    def test_no_url_writes_empty_parquet(self, tmp_path, monkeypatch):
        monkeypatch.delenv("SETTLEMENT_API_URL", raising=False)
        monkeypatch.setenv("PSX_DATA_ROOT", str(tmp_path))

        # Must re-import after env change to pick up new module-level var
        if "settlement_enrichment" in sys.modules:
            del sys.modules["settlement_enrichment"]
        from settlement_enrichment import fetch_and_write

        result = fetch_and_write(date(2026, 5, 25))

        assert isinstance(result, pd.DataFrame)
        assert "settlement_data_available" in result.columns
        # File written even when empty
        parquet_path = tmp_path / "enrichment" / "settlement_2026-05-25.parquet"
        assert parquet_path.exists()

    def test_unreachable_r5_writes_empty_parquet(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SETTLEMENT_API_URL", "http://localhost:19999")  # nothing there
        monkeypatch.setenv("PSX_DATA_ROOT", str(tmp_path))

        if "settlement_enrichment" in sys.modules:
            del sys.modules["settlement_enrichment"]
        from settlement_enrichment import fetch_and_write

        # Must not raise — must write empty Parquet
        result = fetch_and_write(date(2026, 5, 25))

        assert isinstance(result, pd.DataFrame)
        parquet_path = tmp_path / "enrichment" / "settlement_2026-05-25.parquet"
        assert parquet_path.exists()
        if not result.empty:
            assert result["settlement_data_available"].iloc[0] is False

    def test_fetch_is_always_idempotent(self, tmp_path, monkeypatch):
        """Two calls for the same date must produce identical output."""
        monkeypatch.delenv("SETTLEMENT_API_URL", raising=False)
        monkeypatch.setenv("PSX_DATA_ROOT", str(tmp_path))

        if "settlement_enrichment" in sys.modules:
            del sys.modules["settlement_enrichment"]
        from settlement_enrichment import fetch_and_write

        r1 = fetch_and_write(date(2026, 5, 25))
        r2 = fetch_and_write(date(2026, 5, 25))

        pd.testing.assert_frame_equal(r1, r2)
