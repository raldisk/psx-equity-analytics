#!/usr/bin/env bash
# rotate_secrets.sh — PSX Analytics secret rotation stub
#
# Purpose: Placeholder for API key rotation when authentication is added to the
#          serving layer. Currently a no-op because the API has no auth by default
#          (internal deployment model; see docs/adr/ADR-001-duckdb-over-postgres.md).
#
# When auth is added:
#   1. Generate a new API key: openssl rand -hex 32
#   2. Update the Airflow Variable PSX_API_KEY via `airflow variables set`
#   3. Rotate the container env var and trigger a rolling restart
#   4. Invalidate the old key in the gateway config
#
# Usage: ./scripts/rotate_secrets.sh [--dry-run]

set -euo pipefail

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*"; }

log "PSX Analytics secret rotation"
log "Auth model: none (internal deployment) — stub only"

if [[ "$DRY_RUN" == "true" ]]; then
  log "DRY RUN — no changes made"
  exit 0
fi

# Check whether PSX_API_KEY is set at all; if not, rotation is not applicable
if [[ -z "${PSX_API_KEY:-}" ]]; then
  log "PSX_API_KEY not set in environment — no authentication configured. Exiting."
  exit 0
fi

log "PSX_API_KEY is set — rotation would proceed here (implement when auth is added)."
log "No action taken in this stub."
exit 0
