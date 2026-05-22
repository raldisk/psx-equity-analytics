---
name: Feature Request
about: Propose a new capability for PSX Analytics
title: "[FEATURE] "
labels: ["enhancement"]
assignees: []
---

## Feature Description
<!-- What capability do you want to add? -->

## Motivation
<!-- Why is this needed? Which use case does it serve? -->

## Proposed Approach
<!-- High-level implementation idea -->

## P3-Specific Constraints to Consider
- DuckDB version pin (`>=0.10.0,<1.0.0`) — check new API compatibility
- SARIMA must remain optional (statsmodels in `[stats]` extra)
- Serving layer must stay read_only — no writes from FastAPI handlers
- manifest.json is the authority for all raw file paths — do not bypass
- PSX EOD data volume: ~250 symbols × ~250 trading days/year

## Acceptance Criteria
- [ ]
- [ ]
- [ ]

## Does this require a new dbt model or migration?
- [ ] Yes — describe schema changes:
- [ ] No
