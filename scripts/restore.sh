#!/usr/bin/env bash
# restore.sh — PSX Analytics DuckDB restore with checksum validation
#
# Validates the sha256 checksum of a backup file, then restores it to
# the live DuckDB path. Refuses to restore if the checksum does not match.
#
# Usage: ./scripts/restore.sh <backup_file> [--dry-run]
# Env:   PSX_DATA_ROOT  — directory containing psx_analytics.duckdb (default: /opt/airflow/data)

set -euo pipefail

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

BACKUP_FILE="${1:-}"
DRY_RUN=false
if [[ "${2:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

if [[ -z "${BACKUP_FILE}" ]]; then
  echo "Usage: $0 <backup_file> [--dry-run]"
  exit 1
fi

if [[ ! -f "${BACKUP_FILE}" ]]; then
  log "ERROR: Backup file not found: ${BACKUP_FILE}"
  exit 1
fi

CHECKSUM_FILE="${BACKUP_FILE}.sha256"
if [[ ! -f "${CHECKSUM_FILE}" ]]; then
  log "ERROR: Checksum file not found: ${CHECKSUM_FILE}"
  log "Cannot restore without checksum verification. Aborting."
  exit 1
fi

PSX_DATA_ROOT="${PSX_DATA_ROOT:-/opt/airflow/data}"
DB_FILE="${PSX_DATA_ROOT}/psx_analytics.duckdb"

log "Backup source:  ${BACKUP_FILE}"
log "Restore target: ${DB_FILE}"

log "Verifying checksum..."
if ! sha256sum --check "${CHECKSUM_FILE}" --quiet; then
  log "ERROR: Checksum verification FAILED. Backup file is corrupt or tampered."
  exit 1
fi
log "Checksum OK."

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN — restore target would be overwritten; no files written"
  exit 0
fi

# Snapshot the current live DB before overwriting (safety net)
SAFETY_SNAP="${DB_FILE}.pre-restore-$(date -u +%Y%m%dT%H%M%SZ)"
if [[ -f "${DB_FILE}" ]]; then
  log "Snapshotting current DB to ${SAFETY_SNAP}..."
  cp "${DB_FILE}" "${SAFETY_SNAP}"
fi

log "Restoring backup..."
cp "${BACKUP_FILE}" "${DB_FILE}"

log "Restore complete: ${DB_FILE}"
log "Safety snapshot retained at: ${SAFETY_SNAP:-none}"
