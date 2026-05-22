# Changelog — PSX Analytics (P3)

All notable changes to this project will be documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- `pyproject.toml` — unified build system and dependency declaration (`P3-PROP-04`)
- `Makefile` — developer workflow targets: `install`, `test`, `lint`, `format`, `up`, `down`, `load-test` (`P3-PROP-06`)
- `infra/docker/` — multi-stage `Dockerfile`, dev/prod/test compose stack (`P3-PROP-07`)
- `.editorconfig`, `.pre-commit-config.yaml`, `.dockerignore` — developer tooling baseline (`P3-PROP-05`)
- `tests/` — full scaffold: unit, integration, contracts, dq, load, chaos, fixtures (`P3-PROP-08`)
- `tests/load/locustfile.py` — Locust load test targeting p95 < 200ms at 50 users (`P3-PROP-09`)
- `tests/unit/test_properties_api.py` — Hypothesis property tests for `validate_date_range()` (`P3-PROP-10`)
- `.github/workflows/` — CI (Python 3.11/3.12 matrix), CD (GHCR push on tag), security (bandit/safety/trivy) (`P3-PROP-11`)
- `.github/dependabot.yml`, PR template, issue templates (`P3-PROP-11`)
- `dbt/models/staging/schema.yml` — dbt-expectations column contract tests for `stg_psx_eod` (`P3-PROP-15`)
- `dbt/packages.yml` — `calogica/dbt_expectations` package declaration (`P3-PROP-15`)
- `logs/`, `prometheus/`, `grafana/` — observability scaffold with DuckDB metrics exporter and Grafana dashboard (`P3-PROP-12`)
- `security/SECURITY.md` — vulnerability reporting policy and architectural security properties (`P3-PROP-13`)
- `scripts/backup.sh`, `scripts/restore.sh` — DuckDB backup/restore with sha256 checksum verification (`P3-PROP-13`)
- `scripts/rotate_secrets.sh` — secret rotation stub (`P3-PROP-13`)
- `docs/system-design.svg` — end-to-end data flow diagram (`P3-PROP-13`)
- `CONTRIBUTING.md` — branch conventions, Parquet immutability rule, test requirements (`P3-PROP-13`)
- `KNOWN_ISSUES.md` — documented KI-001 through KI-005 (`P3-PROP-13`)
- `docs/adr/ADR-001-duckdb-over-postgres.md`, `ADR-002-airflow-only-scheduling.md` (`P3-PROP-13`)
- `governance/P3-RUNBOOK.md`, `governance/P3-ADVERSARIAL-AUDIT-TEMPLATE.md` (`P3-PROP-13`)

### Fixed
- `.env.example` — file-swap defect repaired; now contains correct PSX env variable template (`P3-PROP-01`)
- `serving/` — removed `app/api.py` ambiguity; canonical entry point is `serving/psx_analytics_api.py` (`P3-PROP-02`)
- `governance/` — removed unprefixed duplicate governance files (`P3-PROP-03`)
- `dbt/models/staging/stg_psx_eod.sql` — replaced `SELECT *` with explicit typed columns in `manifest_source` CTE (`P3-PROP-14`)

### Changed
- `serving/psx_analytics_api.py` — structured JSON logging (`_JsonFormatter`) and optional OTEL tracing injected (`P3-PROP-12`)

---

<!-- Template for future releases:
## [0.2.0] - YYYY-MM-DD
### Added
### Fixed
### Changed
### Removed
-->
