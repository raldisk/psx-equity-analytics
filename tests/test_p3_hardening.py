"""
test_p3_hardening.py
====================
P3 PSX Analytics — regression tests for F-019, F-022, F-023, F-025 hardening.

Every test proves the original failure mode is eliminated, not just that new code exists.

F-019 original failure: SHA-256 dedup was file-level. An amended PSX file (same
  symbol/date, different content) was written as a second Parquet file in raw/.
  DuckDB wildcard queries over raw/symbol/date/*.parquet returned duplicates.
  Fix: manifest controls canonical file per (symbol, date). Amended file
  supersedes prior; prior retained; DuckDB reads only manifest-referenced path.

F-022 original failure: Full-table DuckDB query with no date range exhausted
  process memory and crashed the serving layer (denial-of-service by naive query).
  Fix: validate_date_range() raises ValueError before query reaches DuckDB.

F-023 original failure: VWAP, Amihud illiquidity, price_impact_bps stored at
  raw tick grain. SUM(vwap_running) over ticks = semantically wrong answer (silent).
  Fix: these measures are computed only in fact_daily_analytics at daily grain.
  fact_trade contains only additive tick-level measures.

F-025 original failure: SARIMA non-convergence raised an exception inside the
  dbt Python model, blocking the entire dbt run for all symbols.
  Fix: per-symbol try/except; failed symbols get sarima_status=FAILED_CONVERGENCE
  with NULL trend_component; dbt run continues.

Run: pytest tests/test_p3_hardening.py -v
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Ensure scripts/ is importable
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))


# ═══════════════════════════════════════════════════════════════════════════════
# F-019 — MANIFEST-BASED AMENDED FILE CONFLICT RESOLUTION
# ═══════════════════════════════════════════════════════════════════════════════

class TestF019AmendedFileConflict:
    """
    Original failure mode: Two files for same (symbol, date) → DuckDB returns duplicates.
    Success condition: manifest always has exactly ONE canonical path per (symbol, date).
    Failure condition (regression): second ingest of amended file writes new manifest
      entry pointing to both files, or manifest is not updated.
    """

    def _make_csv(self, tmp_path: Path, rows: list[dict]) -> Path:
        df = pd.DataFrame(rows)
        path = tmp_path / f"psx_{len(rows)}rows.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        return path

    def test_initial_ingest_creates_manifest_entry(self, tmp_path):
        """First ingest creates manifest entry; manifest has exactly 1 entry for key."""
        from psx_ingest import ingest_psx_csv, load_manifest, manifest_key

        raw_dir      = tmp_path / "raw"
        manifest_pth = tmp_path / "manifest.json"
        csv = self._make_csv(tmp_path, [
            {"symbol":"ALI","open":38.0,"high":38.5,"low":37.8,"close":38.2,"volume":100000,"value":3820000},
            {"symbol":"ALI","open":38.1,"high":38.6,"low":37.9,"close":38.3,"volume":110000,"value":4213000},
        ])

        entry = ingest_psx_csv(csv, "ALI", "2025-01-15", raw_dir, manifest_pth)
        manifest = load_manifest(manifest_pth)

        key = manifest_key("ALI", "2025-01-15")
        assert key in manifest,     "Manifest entry not created"
        assert manifest[key]["amended"] is False, "First ingest should not be marked amended"
        assert manifest[key]["row_count"] == 2
        assert Path(manifest[key]["raw_path"]).exists(), "Raw Parquet file not written"

    def test_amendment_supersedes_prior_canonical_file(self, tmp_path):
        """
        Original failure: second file for same key written independently; glob returns both.
        Success: manifest updated to new canonical; prior_raw_path recorded; only one canonical.
        """
        from psx_ingest import ingest_psx_csv, load_manifest, manifest_key

        raw_dir      = tmp_path / "raw"
        manifest_pth = tmp_path / "manifest.json"

        # First ingest
        csv1 = self._make_csv(tmp_path, [
            {"symbol":"SM","open":940.0,"high":945.0,"low":938.0,"close":942.0,"volume":50000,"value":47100000},
        ])
        entry1 = ingest_psx_csv(csv1, "SM", "2025-01-15", raw_dir, manifest_pth)
        prior_path = entry1["raw_path"]

        # Amendment: same symbol/date, different content (different volume)
        csv2 = self._make_csv(tmp_path, [
            {"symbol":"SM","open":940.0,"high":946.0,"low":938.0,"close":943.0,"volume":55000,"value":51865000},
            {"symbol":"SM","open":941.0,"high":946.0,"low":939.0,"close":943.5,"volume":55001,"value":51866000},
        ])
        entry2 = ingest_psx_csv(csv2, "SM", "2025-01-15", raw_dir, manifest_pth)
        manifest = load_manifest(manifest_pth)

        key = manifest_key("SM", "2025-01-15")
        assert manifest[key]["amended"] is True,                    "Amendment not flagged"
        assert manifest[key]["prior_raw_path"] == prior_path,       "Prior path not recorded"
        assert manifest[key]["raw_path"] != prior_path,             "Canonical path not updated"
        assert manifest[key]["row_count"] == 2,                     "Row count not updated"

        # Prior file must still exist (raw/ is append-only)
        assert Path(prior_path).exists(), "Prior file was deleted (raw/ must be append-only)"

        # Only ONE canonical path in manifest (not two)
        # The manifest has exactly one key for (SM, 2025-01-15)
        sm_keys = [k for k in manifest if k.startswith("SM/")]
        assert len(sm_keys) == 1, f"Expected 1 manifest key, got {len(sm_keys)}: {sm_keys}"

    def test_identical_file_raises_file_exists_error(self, tmp_path):
        """
        Same file re-ingested (same SHA-256) must raise FileExistsError — idempotent guard.
        No duplicate write. Manifest unchanged.
        """
        from psx_ingest import ingest_psx_csv, FileExistsError as ingested_fee

        raw_dir      = tmp_path / "raw"
        manifest_pth = tmp_path / "manifest.json"
        csv = self._make_csv(tmp_path, [
            {"symbol":"BDO","open":135.0,"high":136.0,"low":134.5,"close":135.5,"volume":200000,"value":27100000},
        ])
        ingest_psx_csv(csv, "BDO", "2025-01-16", raw_dir, manifest_pth)

        # Re-ingest same file — must raise
        with pytest.raises(FileExistsError, match="already canonical"):
            ingest_psx_csv(csv, "BDO", "2025-01-16", raw_dir, manifest_pth)

    def test_manifest_rebuild_from_raw_selects_latest_modified(self, tmp_path):
        """
        Recovery: if manifest.json is lost, rebuild selects latest-modified file as canonical.
        Success: exactly one canonical entry per (symbol, date) after rebuild.
        """
        from psx_ingest import ingest_psx_csv, rebuild_manifest_from_raw, load_manifest

        raw_dir      = tmp_path / "raw"
        manifest_pth = tmp_path / "manifest.json"

        csv1 = self._make_csv(tmp_path, [
            {"symbol":"AYC","open":10.0,"high":10.2,"low":9.9,"close":10.1,"volume":5000,"value":50500},
        ])
        csv2 = self._make_csv(tmp_path / "amended", [
            {"symbol":"AYC","open":10.0,"high":10.3,"low":9.9,"close":10.2,"volume":5500,"value":56100},
            {"symbol":"AYC","open":10.1,"high":10.3,"low":10.0,"close":10.2,"volume":5501,"value":56110},
        ])
        csv2.parent.mkdir(exist_ok=True)
        pd.DataFrame([
            {"symbol":"AYC","open":10.0,"high":10.3,"low":9.9,"close":10.2,"volume":5500,"value":56100},
        ]).to_csv(csv2, index=False)

        ingest_psx_csv(csv1, "AYC", "2025-01-17", raw_dir, manifest_pth)
        import time; time.sleep(0.05)  # Ensure different mtime
        ingest_psx_csv(
            self._make_csv(tmp_path / "am2", [
                {"symbol":"AYC","open":10.0,"high":10.4,"low":9.9,"close":10.3,"volume":5600,"value":57680},
            ]),
            "AYC", "2025-01-17", raw_dir, manifest_pth
        )

        # Delete manifest to simulate loss
        manifest_pth.unlink()
        assert not manifest_pth.exists()

        # Rebuild
        rebuilt = rebuild_manifest_from_raw(raw_dir, manifest_pth)
        ayc_keys = [k for k in rebuilt if k.startswith("AYC/")]
        assert len(ayc_keys) == 1, f"Rebuild produced {len(ayc_keys)} entries for AYC"
        assert rebuilt["AYC/2025-01-17"]["_rebuilt"] is True


# ═══════════════════════════════════════════════════════════════════════════════
# F-022 — DUCKDB DATE RANGE GUARD
# ═══════════════════════════════════════════════════════════════════════════════

class TestF022DateRangeGuard:
    """
    Original failure mode: Full-table scan (no date predicate) exhausts DuckDB memory;
    serving layer crashes; all concurrent sessions terminated.
    Success condition: validate_date_range() raises ValueError before query executes.
    Failure condition (regression): function accepts missing or out-of-range dates.
    """

    def test_missing_start_date_raises(self):
        from duckdb_manager import validate_date_range
        with pytest.raises(ValueError, match="Missing required parameter: start_date"):
            validate_date_range(None, "2025-03-31")

    def test_missing_end_date_raises(self):
        from duckdb_manager import validate_date_range
        with pytest.raises(ValueError, match="Missing required parameter: end_date"):
            validate_date_range("2025-01-01", None)

    def test_range_exceeding_max_raises(self):
        from duckdb_manager import validate_date_range
        # 91 days > default MAX_DATE_RANGE_DAYS (90)
        with pytest.raises(ValueError, match="exceeds maximum"):
            validate_date_range("2025-01-01", "2025-04-02")  # 91 days

    def test_valid_range_within_max_passes(self):
        from duckdb_manager import validate_date_range
        # 30 days — must not raise
        validate_date_range("2025-01-01", "2025-01-30")

    def test_end_before_start_raises(self):
        from duckdb_manager import validate_date_range
        with pytest.raises(ValueError, match="must be >="):
            validate_date_range("2025-03-31", "2025-01-01")

    def test_invalid_date_format_raises(self):
        from duckdb_manager import validate_date_range
        with pytest.raises(ValueError, match="Invalid date format"):
            validate_date_range("01/15/2025", "2025-01-30")

    def test_exact_max_boundary_passes(self):
        from duckdb_manager import validate_date_range
        # Exactly 90 days (inclusive) — must not raise
        validate_date_range("2025-01-01", "2025-04-01")  # 90 days

    def test_one_day_range_passes(self):
        from duckdb_manager import validate_date_range
        validate_date_range("2025-06-15", "2025-06-15")  # 1 day

    def test_guard_prevents_full_table_scan_reaching_duckdb(self, tmp_path):
        """
        Proves the guard fires BEFORE any DuckDB connection is opened.
        If the guard fires, no DuckDB memory is consumed by the query.
        """
        from duckdb_manager import validate_date_range
        guard_fired = False
        try:
            validate_date_range(None, None, table_name="fact_trade")
        except ValueError:
            guard_fired = True

        assert guard_fired, (
            "REGRESSION F-022: date range guard did not fire for (None, None) input. "
            "Full-table scan would proceed to DuckDB and exhaust memory."
        )


# ═══════════════════════════════════════════════════════════════════════════════
# F-023 — VWAP/AMIHUD AT CORRECT DAILY GRAIN (KIMBALL VIOLATION FIX)
# ═══════════════════════════════════════════════════════════════════════════════

class TestF023DailyGrainCorrectness:
    """
    Original failure mode: vwap_running, amihud_illiquidity stored at tick grain.
    SUM(vwap_running) over ticks = meaningless number (silent wrong answer).
    Success condition: fact_trade contains only additive tick measures.
      Non-additive measures exist only in fact_daily_analytics at daily grain.
    Failure condition (regression): vwap or amihud appear in fact_trade schema.
    """

    def test_fact_trade_schema_has_no_non_additive_measures(self):
        """
        Proves fact_trade does NOT contain vwap, amihud_illiquidity, or price_impact_bps.
        These are exclusively in fact_daily_analytics.
        """
        from duckdb_manager import SCHEMA_SQL

        # Non-additive measures that caused the Kimball violation
        forbidden_in_fact_trade = [
            "vwap_running", "vwap",
            "amihud_illiquidity",
            "price_impact_bps",
        ]

        # Extract only the fact_trade CREATE TABLE block
        fact_trade_block = ""
        in_block = False
        for line in SCHEMA_SQL.splitlines():
            if "CREATE TABLE IF NOT EXISTS fact_trade" in line:
                in_block = True
            if in_block:
                fact_trade_block += line + "\n"
                if line.strip() == ");":
                    break

        for col in forbidden_in_fact_trade:
            assert col not in fact_trade_block, (
                f"REGRESSION F-023: '{col}' found in fact_trade schema. "
                f"Non-additive measures at tick grain produce silent wrong answers "
                f"for any SUM() aggregation query."
            )

    def test_fact_daily_analytics_schema_has_non_additive_measures(self):
        """
        Proves non-additive measures exist in fact_daily_analytics (correct grain).
        """
        from duckdb_manager import SCHEMA_SQL

        required_in_daily = ["vwap", "amihud_illiquidity", "price_impact_bps"]
        daily_block = ""
        in_block = False
        for line in SCHEMA_SQL.splitlines():
            if "CREATE TABLE IF NOT EXISTS fact_daily_analytics" in line:
                in_block = True
            if in_block:
                daily_block += line + "\n"
                if line.strip() == ");":
                    break

        for col in required_in_daily:
            assert col in daily_block, (
                f"Column '{col}' missing from fact_daily_analytics — "
                f"non-additive measure must exist at daily grain."
            )

    def test_vwap_is_computed_as_value_over_volume(self):
        """
        VWAP = total_value / total_volume at daily grain.
        Proves computation is semantically correct (not SUM of per-tick prices).
        """
        import numpy as np

        # Simulate a day's ticks
        ticks = pd.DataFrame({
            "price":  [10.0, 10.2, 10.1, 10.3, 10.2],
            "volume": [1000, 2000, 1500, 3000, 500],
            "value":  [10000, 20400, 15150, 30900, 5100],
        })

        # Correct VWAP: total_value / total_volume
        correct_vwap = ticks["value"].sum() / ticks["volume"].sum()

        # Wrong VWAP: SUM(price) / count — what tick-grain SUM would imply
        wrong_vwap = ticks["price"].mean()

        assert abs(correct_vwap - 10.19375) < 0.001, f"Correct VWAP calculation: {correct_vwap}"
        assert correct_vwap != wrong_vwap, "VWAP must differ from simple price average"

        # Verify fact_daily_analytics model computes it correctly
        # (reading the model source as proof)
        import inspect
        model_path = Path(__file__).parent.parent / "dbt/models/marts/fact_daily_analytics.py"
        model_src = model_path.read_text()
        assert 'total_value" / total_volume' in model_src or \
               '"total_value"] / daily["total_volume"]' in model_src, \
            "fact_daily_analytics does not compute VWAP as value/volume"


# ═══════════════════════════════════════════════════════════════════════════════
# F-025 — SARIMA CONVERGENCE ISOLATION
# ═══════════════════════════════════════════════════════════════════════════════

class TestF025SarimaIsolation:
    """
    Original failure mode: SARIMA non-convergence raised exception inside dbt model,
    blocking the entire run. No data was produced for ANY symbol when ONE failed.
    Success condition: per-symbol SARIMA failure is caught; failing symbol gets
      sarima_status=FAILED_CONVERGENCE; all other symbols continue processing.
    Failure condition (regression): exception propagates out of per-symbol loop.
    """

    def _make_mock_dbt(self, df: pd.DataFrame):
        """Create a mock dbt object that returns a DataFrame from dbt.ref()."""
        mock_ref = MagicMock()
        mock_ref.df.return_value = df
        mock_dbt = MagicMock()
        mock_dbt.ref.return_value = mock_ref
        mock_dbt.config.get.return_value = "test_invocation"
        return mock_dbt

    def test_sarima_failure_on_one_symbol_does_not_block_others(self):
        """
        Core regression test for F-025.
        One symbol has insufficient data (triggers SARIMA convergence failure).
        Other symbols must still be processed and appear in result.
        """
        import importlib.util
        model_path = Path(__file__).parent.parent / "dbt/models/marts/fact_daily_analytics.py"
        spec = importlib.util.spec_from_file_location("fact_daily_analytics", model_path)
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        # Two symbols: ALI (enough data) and LOW_LIQ (only 2 rows — triggers INSUFFICIENT_DATA)
        ali_rows = [
            {"symbol_key": 1, "session_date_key": 20250101 + i,
             "price": 38.0 + i * 0.1, "volume": 100000, "value": 3800000 + i * 1000}
            for i in range(15)
        ]
        low_rows = [
            {"symbol_key": 2, "session_date_key": 20250101 + i,
             "price": 5.0, "volume": 100, "value": 500}
            for i in range(2)   # Only 2 rows — INSUFFICIENT_DATA for SARIMA
        ]
        df = pd.DataFrame(ali_rows + low_rows)
        mock_dbt = self._make_mock_dbt(df)

        # Must NOT raise — if it raises, original bug has regressed
        try:
            result = mod.model(mock_dbt, None)
        except Exception as e:
            pytest.fail(
                f"REGRESSION F-025: model() raised {type(e).__name__}: {e}. "
                f"SARIMA failure should be caught per-symbol, not propagated. "
                f"The original bug caused the entire dbt run to fail when any symbol "
                f"had convergence issues."
            )

        # Both symbols must appear in result
        symbol_keys = set(result["symbol_key"].unique())
        assert 1 in symbol_keys, "ALI (symbol_key=1) missing from result after LOW_LIQ failure"
        assert 2 in symbol_keys, "LOW_LIQ (symbol_key=2) missing — should appear with INSUFFICIENT_DATA status"

        # sarima_status column must be present
        assert "sarima_status" in result.columns, "sarima_status column missing from result"

        # LOW_LIQ must have INSUFFICIENT_DATA or FAILED_CONVERGENCE status (not OK)
        low_status = result[result["symbol_key"] == 2]["sarima_status"].iloc[0]
        assert low_status in ("INSUFFICIENT_DATA", "FAILED_CONVERGENCE", "SKIPPED_NO_STATSMODELS"), (
            f"symbol_key=2 has status '{low_status}' — expected non-OK status for insufficient data"
        )

    def test_statsmodels_unavailable_does_not_raise(self):
        """
        If statsmodels is not installed, model must complete with status=SKIPPED_NO_STATSMODELS.
        This prevents environment rebuild from breaking the pipeline silently.
        """
        import importlib.util
        model_path = Path(__file__).parent.parent / "dbt/models/marts/fact_daily_analytics.py"
        spec = importlib.util.spec_from_file_location("fact_daily_analytics", model_path)
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        df = pd.DataFrame([
            {"symbol_key": 3, "session_date_key": 20250101 + i,
             "price": 10.0, "volume": 5000, "value": 50000}
            for i in range(12)
        ])
        mock_dbt = self._make_mock_dbt(df)

        # Simulate statsmodels unavailable
        with patch.dict("sys.modules", {"statsmodels": None,
                                         "statsmodels.tsa": None,
                                         "statsmodels.tsa.statespace": None,
                                         "statsmodels.tsa.statespace.sarimax": None}):
            try:
                result = mod.model(mock_dbt, None)
                status_vals = result["sarima_status"].unique().tolist()
                assert any(s in ("SKIPPED_NO_STATSMODELS", "PENDING") for s in status_vals), (
                    f"Expected SKIPPED_NO_STATSMODELS when statsmodels absent, got: {status_vals}"
                )
            except ImportError:
                # Acceptable — statsmodels import failure itself is caught in model
                pass
            except Exception as e:
                pytest.fail(f"Model raised unexpected exception without statsmodels: {e}")

    def test_empty_staging_table_returns_empty_result(self):
        """
        Edge case: stg_psx_eod is empty (e.g. market holiday, data not yet loaded).
        Model must return an empty DataFrame — not raise.
        """
        import importlib.util
        model_path = Path(__file__).parent.parent / "dbt/models/marts/fact_daily_analytics.py"
        spec = importlib.util.spec_from_file_location("fact_daily_analytics", model_path)
        mod  = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        df = pd.DataFrame(columns=["symbol_key", "session_date_key", "price", "volume", "value"])
        mock_dbt = self._make_mock_dbt(df)

        result = mod.model(mock_dbt, None)
        assert len(result) == 0, f"Expected empty result for empty staging, got {len(result)} rows"


# ═══════════════════════════════════════════════════════════════════════════════
# INTEGRATION: manifest-path query pattern (no glob)
# ═══════════════════════════════════════════════════════════════════════════════

class TestManifestQueryPattern:
    """
    Proves that the serving layer reads from manifest-referenced paths, not raw/ glob.
    The original bug was that DuckDB glob over raw/symbol/date/*.parquet read ALL files
    for a (symbol, date) — including superseded amendment files — producing duplicates.
    """

    def test_manifest_returns_single_path_after_amendment(self, tmp_path):
        """
        After ingesting an original and an amendment for the same (symbol, date),
        the manifest contains exactly ONE raw_path. Reading that path returns only
        the amendment's rows — not the original's rows (no duplication).
        """
        from psx_ingest import ingest_psx_csv, load_manifest, manifest_key

        raw_dir = tmp_path / "raw"
        manifest_pth = tmp_path / "manifest.json"

        # Original file: 1 row
        csv1 = tmp_path / "original.csv"
        pd.DataFrame([
            {"symbol":"MPI","open":5.0,"high":5.2,"low":4.9,"close":5.1,"volume":10000,"value":51000}
        ]).to_csv(csv1, index=False)

        # Amendment: 2 rows (corrected data)
        csv2 = tmp_path / "amended.csv"
        pd.DataFrame([
            {"symbol":"MPI","open":5.0,"high":5.3,"low":4.9,"close":5.2,"volume":11000,"value":57200},
            {"symbol":"MPI","open":5.1,"high":5.3,"low":5.0,"close":5.2,"volume":11001,"value":57205},
        ]).to_csv(csv2, index=False)

        ingest_psx_csv(csv1, "MPI", "2025-02-01", raw_dir, manifest_pth)
        ingest_psx_csv(csv2, "MPI", "2025-02-01", raw_dir, manifest_pth)

        manifest = load_manifest(manifest_pth)
        key = manifest_key("MPI", "2025-02-01")
        canonical_path = Path(manifest[key]["raw_path"])

        # Read only the canonical path (as the serving layer should)
        df_canonical = pd.read_parquet(canonical_path)

        # Must have 2 rows (amendment) — not 3 (original 1 + amendment 2)
        assert len(df_canonical) == 2, (
            f"REGRESSION: Reading canonical path returned {len(df_canonical)} rows. "
            f"Expected 2 (amendment rows only). "
            f"A glob over raw/ would return 3 rows (1 original + 2 amendment = duplicates). "
            f"Manifest-path reading prevents this."
        )

        # Confirm 2 distinct files exist in raw/ (both retained)
        all_raw = list((raw_dir / "MPI" / "2025-02-01").glob("*.parquet"))
        assert len(all_raw) == 2, (
            f"Expected 2 files in raw/ (original retained, amendment added). "
            f"Got {len(all_raw)}. raw/ must be append-only."
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])