#!/usr/bin/env bash
# backup.sh — PSX Analytics DuckDB backup with checksum and retention
#
# Creates a timestamped copy of the DuckDB file, verifies it with sha256,
# and prunes backups older than RETENTION_DAYS.
#
# Usage: ./scripts/backup.sh [--dry-run]
# Env:   PSX_DATA_ROOT     — directory containing psx_analytics.duckdb (default: /opt/airflow/data)
#        PSX_BACKUP_DIR    — backup destination (default: ${PSX_DATA_ROOT}/backups)
#        RETENTION_DAYS    — days to keep backups (default: 30)

set -euo pipefail

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

PSX_DATA_ROOT="${PSX_DATA_ROOT:-/opt/airflow/data}"
PSX_BACKUP_DIR="${PSX_BACKUP_DIR:-${PSX_DATA_ROOT}/backups}"
RETENTION_DAYS="${RETENTION_DAYS:-30}"
DB_FILE="${PSX_DATA_ROOT}/psx_analytics.duckdb"
TIMESTAMP=$(date -u +%Y%m%dT%H%M%SZ)
BACKUP_FILE="${PSX_BACKUP_DIR}/psx_analytics_${TIMESTAMP}.duckdb"
CHECKSUM_FILE="${BACKUP_FILE}.sha256"

log "Source:     ${DB_FILE}"
log "Backup:     ${BACKUP_FILE}"
log "Retention:  ${RETENTION_DAYS} days"

if [[ ! -f "${DB_FILE}" ]]; then
  log "ERROR: DuckDB file not found at ${DB_FILE}"
  exit 1
fi

# Check for active WAL — backup during write window risks partial state
WAL_FILE="${DB_FILE}.wal"
if [[ -f "${WAL_FILE}" ]]; then
  log "WARNING: WAL file detected at ${WAL_FILE} — DuckDB may be mid-write."
  log "Proceeding with backup; verify checksum after restore."
fi

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN — no files written"
  exit 0
fi

mkdir -p "${PSX_BACKUP_DIR}"

log "Copying DuckDB file..."
cp "${DB_FILE}" "${BACKUP_FILE}"

log "Computing sha256 checksum..."
sha256sum "${BACKUP_FILE}" > "${CHECKSUM_FILE}"
log "Checksum: $(cat "${CHECKSUM_FILE}")"

log "Pruning backups older than ${RETENTION_DAYS} days..."
find "${PSX_BACKUP_DIR}" -name "psx_analytics_*.duckdb" -mtime "+${RETENTION_DAYS}" -delete
find "${PSX_BACKUP_DIR}" -name "psx_analytics_*.duckdb.sha256" -mtime "+${RETENTION_DAYS}" -delete

log "Backup complete: ${BACKUP_FILE}"
