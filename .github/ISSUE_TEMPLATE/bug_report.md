---
name: Bug Report
about: Report a defect in PSX Analytics pipeline or serving layer
title: "[BUG] "
labels: ["bug"]
assignees: []
---

## Bug Description
<!-- Clear description of the observed behavior -->

## Expected Behavior
<!-- What should happen instead -->

## Reproduction Steps
1.
2.
3.

## Environment
- DuckDB version: <!-- e.g. duckdb==0.10.2 -->
- Python version: <!-- e.g. 3.11.8 -->
- PSX_DATA_ROOT: <!-- path format, not actual data -->
- SARIMA enabled: <!-- PSX_SARIMA_ENABLED=1 or 0 -->

## P3-Specific Context
- [ ] Affects manifest parsing / F-019 path resolution
- [ ] Affects serving layer (FastAPI / DuckDB read_only)
- [ ] Affects dbt staging model (`stg_psx_eod`)
- [ ] Affects SARIMA pipeline step
- [ ] Affects amendment / F-021 logic

## Logs / Error Output
```
# Paste relevant log lines (JSON format preferred — set LOG_FORMAT=json)
```

## Parquet / Manifest State (if applicable)
<!-- Describe manifest.json state without including actual market data -->
