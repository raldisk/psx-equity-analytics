"""
psx_ingest.py
=============
PSX EOD CSV ingestion pipeline.

Hardens three adversarial findings:

F-019 (HIGH): SHA-256 dedup was file-level only. PSX amends daily files —
  a new file for `symbol/date` has a different hash but represents the same
  period. Without conflict resolution, raw/ accumulates multiple files for
  the same (symbol, date) and DuckDB wildcard queries return duplicates.
  Fix: an Ingest Manifest (manifest.json) tracks the canonical Parquet file
  per (symbol, session_date). New ingest for an existing key supersedes the
  prior file. DuckDB queries read only manifest-referenced files — never glob.

F-021 (MEDIUM): raw/ immutability via file permission is not a contractual
  guarantee. A chmod can be bypassed by root.
  Fix: a manifest-based canonical tracking layer means the query layer never
  depends on raw/ immutability. Even if a file is physically overwritten, the
  manifest controls which file is read. Physical file immutability is a defense-
  in-depth control; manifest authority is the actual enforcement layer.

F-024 (MEDIUM): computed/ has no versioning. After a corporate action (stock
  split, rights offering), all historical computed/ must be regenerated.
  Without versioning, prior analysis is irreproducible.
  Fix: computed/ is versioned as computed/v{N}/ where N = corporate action
  sequence number. Manifest tracks current canonical version per (symbol, date).
  Prior versions are retained for audit. FastAPI can serve any version.

State authority:
  manifest.json is the AUTHORITATIVE state for which file is canonical.
  raw/ files are immutable once written. No raw/ file is ever deleted or
  overwritten — amendments create a new file. The manifest is the only
  mutable component.

Recovery contract:
  If manifest.json is lost: rebuild from raw/ directory by taking the
  latest-modified file per (symbol, date) key. This is a full rebuild,
  not a data loss event — all raw Parquet files are preserved.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

MANIFEST_FILENAME  = "manifest.json"
RAW_DIR            = Path("data/raw")
COMPUTED_DIR       = Path("data/computed")
MANIFEST_PATH      = Path("data/manifest.json")

# PSX EOD CSV expected columns (minimum required)
REQUIRED_COLUMNS = {
    "symbol", "open", "high", "low", "close", "volume", "value"
}

# ─── Manifest operations ──────────────────────────────────────────────────────

def load_manifest(manifest_path: Path = MANIFEST_PATH) -> dict:
    """
    Load the ingest manifest. Returns empty dict if manifest does not exist.
    Manifest schema:
    {
        "<symbol>/<session_date>": {
            "raw_path":        "data/raw/symbol/date/filename.parquet",
            "sha256":          "abc123...",
            "ingested_at":     "2025-01-15T09:00:00+08:00",
            "row_count":       1234,
            "amended":         false,          # true if this supersedes a prior file
            "prior_raw_path":  null,           # path of superseded file (if amended)
            "computed_version": 0              # current computed/ version for this key
        },
        ...
    }
    The manifest is the AUTHORITATIVE source of which file to read for each key.
    DuckDB queries MUST use manifest paths, not raw/ glob.
    """
    if not manifest_path.exists():
        return {}
    with open(manifest_path) as f:
        return json.load(f)


def save_manifest(manifest: dict, manifest_path: Path = MANIFEST_PATH) -> None:
    """Atomically write manifest to disk. Write to .tmp then rename for atomicity."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = manifest_path.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    tmp.rename(manifest_path)
    log.info("Manifest saved: %d entries", len(manifest))


def manifest_key(symbol: str, session_date: str) -> str:
    """Canonical manifest key: symbol/YYYY-MM-DD."""
    return f"{symbol}/{session_date}"


# ─── CSV → Parquet ingestion ──────────────────────────────────────────────────

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def validate_psx_csv(csv_path: Path) -> tuple[bool, str]:
    """
    Validate PSX EOD CSV before ingestion.
    Returns (valid: bool, reason: str).
    Partial mitigation for F-020: checks column presence and row count > 0.
    Does NOT guarantee completeness (no expected symbol count available).
    """
    try:
        df = pd.read_csv(csv_path, nrows=0)
        cols = {c.lower().strip() for c in df.columns}
        missing = REQUIRED_COLUMNS - cols
        if missing:
            return False, f"Missing required columns: {missing}"
        # Full read for row count
        df_full = pd.read_csv(csv_path)
        if len(df_full) == 0:
            return False, "Empty file — zero rows"
        return True, f"Valid: {len(df_full)} rows"
    except Exception as e:
        return False, f"Parse error: {e}"


def ingest_psx_csv(
    csv_path: Path,
    symbol: str,
    session_date: str,
    raw_dir: Path = RAW_DIR,
    manifest_path: Path = MANIFEST_PATH,
) -> dict:
    """
    Ingest a single PSX EOD CSV file for (symbol, session_date).

    F-019 Fix: If a prior file exists for this (symbol, session_date), this is
    an amendment. The prior file is retained (raw/ is append-only), the manifest
    is updated to point to the new file as canonical, and the prior path is
    recorded in `prior_raw_path` for audit.

    Returns: the new manifest entry for this key.

    Raises:
        ValueError: if CSV fails validation
        FileExistsError: if the exact same file (same SHA-256) is already canonical
    """
    # ── Validate ─────────────────────────────────────────────────────────────
    valid, reason = validate_psx_csv(csv_path)
    if not valid:
        raise ValueError(f"CSV validation failed for {csv_path.name}: {reason}")
    log.info("CSV validation passed: %s — %s", csv_path.name, reason)

    # ── Compute SHA-256 of source CSV ────────────────────────────────────────
    sha256 = _sha256(csv_path)

    # ── Load manifest and check for existing entry ───────────────────────────
    manifest = load_manifest(manifest_path)
    key      = manifest_key(symbol, session_date)
    existing = manifest.get(key)

    if existing and existing["sha256"] == sha256:
        log.info(
            "Skipping: identical file already canonical for %s (SHA-256 match)", key
        )
        raise FileExistsError(
            f"File for {key} is already canonical (same SHA-256: {sha256[:16]}...)."
            f" No ingest needed."
        )

    is_amendment = existing is not None
    if is_amendment:
        log.warning(
            "AMENDMENT DETECTED: %s already has a canonical file (%s). "
            "New file will supersede. Prior file retained at %s.",
            key,
            existing["raw_path"],
            existing["raw_path"],
        )

    # ── Write to raw/ ─────────────────────────────────────────────────────────
    # Each ingestion creates a unique timestamped Parquet file.
    # Multiple files can exist for the same (symbol, date) key.
    # Canonical file is determined by manifest only — not by filesystem state.
    ingest_ts     = datetime.now().strftime("%Y%m%dT%H%M%S")
    parquet_dir   = raw_dir / symbol / session_date
    parquet_dir.mkdir(parents=True, exist_ok=True)
    parquet_path  = parquet_dir / f"{symbol}_{session_date}_{ingest_ts}.parquet"

    df = pd.read_csv(csv_path)
    df.columns = [c.lower().strip() for c in df.columns]
    df["symbol"]       = symbol
    df["session_date"] = session_date
    df["_ingested_at"] = datetime.now().isoformat()
    df["_source_file"] = csv_path.name
    df["_sha256"]      = sha256

    df.to_parquet(parquet_path, index=False, compression="snappy")
    log.info("Written to raw: %s (%d rows)", parquet_path, len(df))

    # ── Update manifest ───────────────────────────────────────────────────────
    entry = {
        "raw_path":        str(parquet_path),
        "sha256":          sha256,
        "ingested_at":     datetime.now().isoformat() + "+08:00",
        "row_count":       len(df),
        "amended":         is_amendment,
        "prior_raw_path":  existing["raw_path"] if is_amendment else None,
        "computed_version": existing.get("computed_version", 0) if existing else 0,
    }
    manifest[key] = entry
    save_manifest(manifest, manifest_path)

    log.info(
        "Manifest updated: key=%s amended=%s row_count=%d",
        key, is_amendment, len(df)
    )
    return entry


# ─── Computed/ versioning ─────────────────────────────────────────────────────

def create_computed_version(
    symbol: str,
    session_date: str,
    computed_df: pd.DataFrame,
    corporate_action_reason: str,
    computed_dir: Path = COMPUTED_DIR,
    manifest_path: Path = MANIFEST_PATH,
) -> Path:
    """
    F-024 Fix: Write a new versioned computed/ Parquet file.

    computed/ is versioned as computed/v{N}/symbol/date/filename.parquet
    where N = corporate action sequence number. Prior versions are retained.
    The manifest tracks the current canonical version per (symbol, date).

    Callers:
      - Initial computed/ generation: version = 0
      - Post-corporate-action adjustment: version = prior_version + 1

    Returns: path to the new versioned Parquet file.
    """
    manifest  = load_manifest(manifest_path)
    key       = manifest_key(symbol, session_date)
    existing  = manifest.get(key, {})

    prior_version  = existing.get("computed_version", -1)
    new_version    = prior_version + 1

    version_dir   = computed_dir / f"v{new_version}" / symbol / session_date
    version_dir.mkdir(parents=True, exist_ok=True)
    ingest_ts     = datetime.now().strftime("%Y%m%dT%H%M%S")
    output_path   = version_dir / f"{symbol}_{session_date}_v{new_version}_{ingest_ts}.parquet"

    computed_df["_computed_version"]        = new_version
    computed_df["_corporate_action_reason"] = corporate_action_reason
    computed_df["_computed_at"]             = datetime.now().isoformat() + "+08:00"
    computed_df.to_parquet(output_path, index=False, compression="snappy")

    # Update manifest with new canonical computed version
    if key not in manifest:
        manifest[key] = {}
    manifest[key]["computed_version"]      = new_version
    manifest[key]["computed_path"]         = str(output_path)
    manifest[key]["last_corporate_action"] = corporate_action_reason
    manifest[key]["computed_updated_at"]   = datetime.now().isoformat() + "+08:00"
    save_manifest(manifest, manifest_path)

    log.info(
        "Computed v%d written: %s reason=%s", new_version, output_path, corporate_action_reason
    )
    return output_path


def get_canonical_computed_path(
    symbol: str,
    session_date: str,
    version: Optional[int] = None,
    manifest_path: Path = MANIFEST_PATH,
) -> Optional[Path]:
    """
    Return the canonical computed/ Parquet path for (symbol, date).
    If version=None: returns current canonical version.
    If version=N: returns the versioned path for historical replay.

    F-024 guarantees: a fund manager study run before a corporate action can
    be reproduced by passing version=N where N was current at study time.
    """
    manifest = load_manifest(manifest_path)
    key      = manifest_key(symbol, session_date)
    entry    = manifest.get(key)

    if entry is None:
        return None

    if version is None:
        # Current canonical version
        return Path(entry["computed_path"]) if "computed_path" in entry else None

    # Historical version lookup — scan version directories
    v_dir = COMPUTED_DIR / f"v{version}" / symbol / session_date
    if not v_dir.exists():
        return None
    files = sorted(v_dir.glob("*.parquet"))
    return files[-1] if files else None


# ─── Manifest rebuild (recovery path) ────────────────────────────────────────

def rebuild_manifest_from_raw(
    raw_dir: Path = RAW_DIR,
    manifest_path: Path = MANIFEST_PATH,
) -> dict:
    """
    Recovery path: rebuild manifest from raw/ directory when manifest.json is lost.
    For each (symbol, date), selects the LATEST-MODIFIED Parquet file as canonical.
    Logs each selection as an amendment (since we cannot know original order).

    This is a data-safe recovery operation — no raw/ files are deleted or modified.
    """
    manifest = {}
    for parquet_path in sorted(raw_dir.rglob("*.parquet")):
        # Expected path: raw/SYMBOL/YYYY-MM-DD/filename.parquet
        parts = parquet_path.relative_to(raw_dir).parts
        if len(parts) < 3:
            continue
        symbol, session_date = parts[0], parts[1]
        key = manifest_key(symbol, session_date)

        existing = manifest.get(key)
        this_mtime = parquet_path.stat().st_mtime

        if existing is None or this_mtime > existing["_mtime"]:
            manifest[key] = {
                "raw_path":       str(parquet_path),
                "sha256":         _sha256(parquet_path),
                "ingested_at":    datetime.fromtimestamp(this_mtime).isoformat() + "+08:00",
                "row_count":      None,  # Not computed during rebuild for speed
                "amended":        existing is not None,
                "prior_raw_path": existing["raw_path"] if existing else None,
                "computed_version": 0,
                "_mtime":         this_mtime,
                "_rebuilt":       True,
            }

    # Remove internal _mtime key before saving
    for entry in manifest.values():
        entry.pop("_mtime", None)

    save_manifest(manifest, manifest_path)
    log.info("Manifest rebuilt from raw/: %d canonical entries", len(manifest))
    return manifest