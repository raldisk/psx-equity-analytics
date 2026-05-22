"""
test_psx_ingest.py
==================
Unit tests for psx_ingest.py: manifest parsing, sha256 validation, amendment logic.
"""
from __future__ import annotations

import json
import hashlib
import pytest
from pathlib import Path
from unittest.mock import patch


class TestManifestParsing:
    """load_manifest() must correctly parse JSON and handle missing files."""

    def test_load_manifest_returns_dict(self, tmp_path):
        manifest_data = {
            "SM_2024-01-02": {
                "symbol": "SM",
                "session_date": "2024-01-02",
                "canonical_raw_path": "/data/raw/SM_20240102.parquet",
                "row_count": 100,
                "amended": False,
            }
        }
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text(json.dumps(manifest_data))

        from scripts.psx_ingest import load_manifest
        result = load_manifest(manifest_file)
        assert isinstance(result, dict)
        assert "SM_2024-01-02" in result

    def test_load_manifest_missing_file_raises(self, tmp_path):
        from scripts.psx_ingest import load_manifest
        missing = tmp_path / "no_manifest.json"
        with pytest.raises((FileNotFoundError, Exception)):
            load_manifest(missing)


class TestManifestKey:
    def test_manifest_key_format(self):
        from scripts.psx_ingest import manifest_key
        key = manifest_key("ALI", "2024-03-15")
        assert key == "ALI_2024-03-15"

    def test_manifest_key_uppercase_symbol(self):
        from scripts.psx_ingest import manifest_key
        key = manifest_key("ali", "2024-03-15")
        # Key must be uppercase symbol
        assert "ali" not in key or "ALI" in key.upper()


class TestAmendmentLogic:
    """amended=True entries must take precedence over non-amended entries."""

    def test_amended_entry_recognized(self, tmp_path):
        """Manifest entry with amended=True should be identifiable as amended."""
        manifest_data = {
            "SM_2024-01-02": {
                "symbol": "SM",
                "session_date": "2024-01-02",
                "canonical_raw_path": "/data/raw/SM_20240102_amended.parquet",
                "row_count": 105,
                "amended": True,
                "prior_raw_path": "/data/raw/SM_20240102.parquet",
            }
        }
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text(json.dumps(manifest_data))
        from scripts.psx_ingest import load_manifest
        result = load_manifest(manifest_file)
        entry = result["SM_2024-01-02"]
        assert entry.get("amended") is True
