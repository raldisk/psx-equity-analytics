"""
settlement_enrichment.py
========================
Optional pre-step: fetch daily bilateral interbank settlement PHP flow
from iso20022-settlement-engine (R5) and write a Parquet enrichment file.

Design contract:
  - ALWAYS writes a Parquet file, even on failure (empty schema on failure).
  - NEVER raises an exception to the Airflow task layer.
  - Writes to {PSX_DATA_ROOT}/enrichment/settlement_{date}.parquet
  - Called by Airflow psx_pipeline_dag.py settlement_enrichment task.

Architecture rationale (DDIA):
  Availability over consistency — if R5 is unreachable, write empty
  enrichment and let the pipeline continue. A NULL settlement column in
  fact_daily_analytics is preferable to a blocked pipeline.

Interface:
  fetch_and_write(target_date: date) -> pd.DataFrame
  Returns the written DataFrame (empty schema on failure).

Environment variables:
  SETTLEMENT_API_URL (Optional) — base URL for R5 FastAPI.
    e.g. http://localhost:8002
    Absence disables enrichment entirely (empty Parquet written).
  PSX_DATA_ROOT (Optional) — root data directory.
    Defaults to ./data relative to the scripts/ directory.
"""
from __future__ import annotations

import logging
import os
import sys
from datetime import date
from pathlib import Path

import httpx
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — sourced from env vars, never from hard-coded defaults
# ---------------------------------------------------------------------------

SETTLEMENT_API_URL: str | None = os.getenv("SETTLEMENT_API_URL")
_DATA_ROOT = Path(os.getenv("PSX_DATA_ROOT", str(Path(__file__).parent.parent / "data")))
_ENRICHMENT_DIR = _DATA_ROOT / "enrichment"
_REQUEST_TIMEOUT = int(os.getenv("SETTLEMENT_API_TIMEOUT", "10"))

# ---------------------------------------------------------------------------
# Canonical empty schema — written on any failure path.
# Downstream dbt model always reads a valid Parquet with correct columns.
# ---------------------------------------------------------------------------

_EMPTY_SCHEMA: dict = {
    "settlement_date": pd.Series(dtype="str"),
    "total_php_flow": pd.Series(dtype="float64"),
    "participant_count": pd.Series(dtype="int64"),
    "cycle_count": pd.Series(dtype="int64"),
    "settlement_data_available": pd.Series(dtype="bool"),
}


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(_EMPTY_SCHEMA)


def fetch_and_write(target_date: date) -> pd.DataFrame:
    """
    Fetch settlement data for target_date from R5 and write Parquet.

    Always returns a DataFrame. Always writes a Parquet file.
    Never raises — exceptions are caught and logged as WARNING.

    Args:
        target_date: The PSX session date to enrich.

    Returns:
        DataFrame written to disk (empty on failure or absent config).
    """
    _ENRICHMENT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = _ENRICHMENT_DIR / f"settlement_{target_date}.parquet"

    # ── Path 1: enrichment disabled — SETTLEMENT_API_URL not configured ──
    if not SETTLEMENT_API_URL:
        logger.info(
            "[settlement_enrichment] SETTLEMENT_API_URL not set — "
            "writing empty Parquet. R4 continues without settlement enrichment."
        )
        df = _empty_frame()
        df.to_parquet(out_path, index=False)
        return df

    # ── Path 2: attempt live fetch from R5 ─────────────────────────────────
    try:
        resp = httpx.get(
            f"{SETTLEMENT_API_URL.rstrip('/')}/settlement/daily",
            params={"settlement_date": str(target_date)},
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()

        df = pd.DataFrame([{
            "settlement_date": str(payload["settlement_date"]),
            "total_php_flow": (
                float(payload["total_php_flow"])
                if payload.get("total_php_flow") is not None
                else None
            ),
            "participant_count": int(payload.get("participant_count", 0)),
            "cycle_count": int(payload.get("cycle_count", 0)),
            "settlement_data_available": payload.get("total_php_flow") is not None,
        }])

        df.to_parquet(out_path, index=False)
        logger.info(
            "[settlement_enrichment] Written enrichment for %s: "
            "total_php_flow=%s, participants=%d",
            target_date,
            payload.get("total_php_flow"),
            payload.get("participant_count", 0),
        )
        return df

    except Exception as exc:  # noqa: BLE001 — intentional broad catch; must not raise
        # ── Path 3: live fetch failed — write empty Parquet, log warning ──
        logger.warning(
            "[settlement_enrichment] Non-fatal fetch failure for %s: %s. "
            "Writing empty Parquet. dbt mart proceeds with NULL settlement columns.",
            target_date,
            exc,
        )
        df = _empty_frame()
        df.to_parquet(out_path, index=False)
        return df


# ---------------------------------------------------------------------------
# CLI entry point — usable independently of Airflow for manual backfill
# python scripts/settlement_enrichment.py 2026-05-25
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")
    target = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today()
    result = fetch_and_write(target)
    print(f"Written {len(result)} row(s) for {target}")
