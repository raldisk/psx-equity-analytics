# Contributing — PSX Analytics (P3)

## Branch Conventions

| Branch pattern | Purpose |
|---|---|
| `main` | Protected. Merge via PR only. CI must pass. |
| `feat/<short-description>` | New feature or proposal implementation |
| `fix/<issue-or-description>` | Bug fix |
| `refactor/<description>` | Code change with no functional impact |
| `docs/<description>` | Documentation only |

Direct pushes to `main` are blocked by the `no-commit-to-branch` pre-commit hook.

## Development Setup

```bash
git clone <repo>
cd ph-psx-microstructure
pip install -e ".[dev,stats]"
pre-commit install
cp .env.example .env
# Edit .env: set PSX_DATA_ROOT to a local data directory
make test
```

## Parquet Immutability Rule

Raw Parquet files under `data/raw/` are **write-once**. Once a file is ingested and registered in `manifest.json`, it must not be modified in place. If a source correction arrives (amended data), the pipeline creates a new file with an `_amended` suffix and updates `manifest.json` to point to it — it does not overwrite the original. This is enforced at the manifest layer (`F-021`), not by filesystem permissions.

Violations of this rule produce silent duplicate rows in `stg_psx_eod` and corrupt the analytics layer.

## Test Requirements

Every PR must pass the full test suite with ≥80% coverage:

```bash
make test       # runs pytest with coverage gate
make lint       # runs pre-commit on all files
```

New code without corresponding tests will not be merged. Tests belong in the appropriate subdirectory under `tests/`:

- Business logic and API endpoints → `tests/unit/`
- DuckDB connectivity and DAG import → `tests/integration/`
- Schema and column contract assertions → `tests/contracts/`
- Data quality threshold validation → `tests/dq/`
- Property-based invariant testing → `tests/unit/test_properties_api.py`

## DuckDB Version Constraint

The project pins DuckDB to `>=0.10.0,<1.0.0`. Do not upgrade past the upper bound without running the full regression suite and updating the pin in both `requirements.txt` and `pyproject.toml`. See `docs/adr/ADR-001-duckdb-over-postgres.md` for the rationale behind the conservative version policy.

## SARIMA Dependency

`statsmodels` is in the `[stats]` optional extra. The pipeline degrades gracefully when it is absent (`sarima_status = SKIPPED_NO_STATSMODELS`). Do not move `statsmodels` to a required dependency.

## Commit Message Format

```
<type>(<scope>): <short description>

<optional body>
```

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `ci`, `chore`. Scope is the affected module (`ingest`, `serving`, `dbt`, `tests`, `infra`).
