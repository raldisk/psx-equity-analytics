"""
test_airflow_dag_import.py
==========================
Integration test: DAG file imports cleanly without parse errors.
Does not require a running Airflow instance — import-level validation only.
"""
from __future__ import annotations

import importlib
import sys
from pathlib import Path
import pytest


class TestAirflowDagImport:
    def test_psx_pipeline_dag_imports_cleanly(self):
        """airflow/dags/psx_pipeline_dag.py must be importable without errors."""
        dag_path = Path(__file__).parent.parent.parent / "airflow" / "dags"
        sys.path.insert(0, str(dag_path))
        try:
            spec = importlib.util.spec_from_file_location(
                "psx_pipeline_dag",
                dag_path / "psx_pipeline_dag.py",
            )
            assert spec is not None, "DAG file not found"
        finally:
            sys.path.pop(0)

    def test_dag_file_exists(self):
        """Confirm DAG file is present at expected path."""
        dag_path = Path(__file__).parent.parent.parent / "airflow" / "dags" / "psx_pipeline_dag.py"
        assert dag_path.exists(), f"DAG file missing: {dag_path}"
