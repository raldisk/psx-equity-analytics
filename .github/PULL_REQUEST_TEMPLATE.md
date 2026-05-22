# Pull Request

## Summary
<!-- What does this PR change and why? -->

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Refactoring (no functional change)
- [ ] Documentation
- [ ] CI/infrastructure

## P3-Specific Checklist

### Data Contract
- [ ] No `SELECT *` in dbt staging models (rule 1)
- [ ] No glob patterns over `raw/` (F-019)
- [ ] manifest.json is the canonical path authority (F-021)
- [ ] All fact-table API endpoints call `validate_date_range()` before DuckDB query (F-022)

### DuckDB Concurrency
- [ ] Serving layer uses `serving_connection()` (read_only=True)
- [ ] Pipeline writes use `pipeline_connection()` only
- [ ] No concurrent read+write paths introduced

### Dependencies
- [ ] DuckDB version pin maintained (`>=0.10.0,<1.0.0`)
- [ ] SARIMA dependency is optional (statsmodels in `[stats]` extra only)
- [ ] New packages added to both `requirements.txt` and `pyproject.toml`

### Tests
- [ ] New code covered by unit tests
- [ ] Coverage gate (80%) passes locally: `make test`
- [ ] Pre-commit passes: `make lint`

## Test Evidence
```
# Paste pytest output here
```

## DuckDB Version Tested
<!-- e.g. duckdb==0.10.2 -->
