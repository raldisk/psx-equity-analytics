# P3 Hardening Log
## PSX Equity Market Microstructure Analytics Platform

> Governance authority: FINAL-V10-e2e-workflow.md  
> Append-only: new entries are appended; prior entries are never modified or deleted.  
> Chronology: adversarial findings → implementation → regression validation → closure.

---

## Adversarial Findings — Source Record

All findings sourced from adversarial-audit.md (P3 section, Findings 019–026 and GSR-005).

| Finding | Severity | Category | Description |
|---|---|---|---|
| F-019 | HIGH | RESOLVABLE | SHA-256 dedup file-level only: PSX amendment produces second Parquet file for same (symbol, date); DuckDB glob returns duplicates |
| F-020 | HIGH | ACCEPTED-RISK | "Daily files complete by design" — unverifiable; no expected symbol count for completeness check |
| F-021 | MEDIUM | RESOLVABLE | `raw/` immutability via file permission only — root bypass possible; not a contractual guarantee |
| F-022 | HIGH | RESOLVABLE | DuckDB unbounded memory: unguarded full-table scan (no date predicate) exhausts process memory, crashes serving layer |
| F-023 | HIGH | REQUIRES-REVISION | VWAP, Amihud illiquidity, price_impact_bps stored at raw tick grain — Kimball violation; SUM() produces silent wrong answers |
| F-024 | MEDIUM | RESOLVABLE | `computed/` has no versioning — post-corporate-action adjustments make prior analyses irreproducible |
| F-025 | MEDIUM | RESOLVABLE | SARIMA non-convergence raises exception inside dbt Python model — blocks entire dbt run for all symbols |
| F-026 | MEDIUM | ACCEPTED-RISK | FIX feed listed as "optional" — without it, microstructure analytics degrade to daily-resolution proxies only |
| GSR-005 | MEDIUM | RESOLVABLE | DuckDB single-writer lock: concurrent dbt transform + Streamlit query blocks each other |

---

## Hardening Implementation Record

### F-019 + F-021 + F-024 Resolution
**File:** `scripts/psx_ingest.py`  
**Status:** IMPLEMENTED + REGRESSION-TESTED

**F-019:** Manifest-based canonical tracking. `manifest.json` holds exactly one `raw_path` per `(symbol, session_date)` key. `ingest_psx_csv()` detects amendment (existing key with different SHA-256), records the prior path in `prior_raw_path`, updates `raw_path` to the new file, sets `amended=True`. Prior file is retained (raw/ is append-only). `rebuild_manifest_from_raw()` provides recovery when manifest is lost.

**F-021:** Manifest authority supersedes filesystem permissions. The serving layer reads only manifest-referenced paths — never globs raw/. Physical immutability via chmod is defense-in-depth, not the enforcement layer.

**F-024:** `create_computed_version()` writes to `computed/v{N}/symbol/date/`. Manifest tracks `computed_version` and `computed_path` per key. Prior versions retained. `get_canonical_computed_path(version=N)` enables historical replay for reproducibility.

**Production defect fixed during implementation:** Ingest timestamp was second-precision (`%Y%m%dT%H%M%S`). Same-second ingests produced identical filenames, causing `OSError` before SHA-256 check. Fixed to millisecond precision: `strftime("%Y%m%dT%H%M%S") + strftime("%f")[:4]`. Applied to both `ingest_psx_csv()` and `create_computed_version()`.

---

### F-022 + GSR-005 Resolution
**File:** `scripts/duckdb_manager.py`  
**Status:** IMPLEMENTED + REGRESSION-TESTED

**F-022:** `validate_date_range()` enforces mandatory date-range predicates on all fact-table queries before any DuckDB connection is opened. Raises `ValueError` for: missing `start_date` or `end_date`, range exceeding `PSX_MAX_DATE_RANGE_DAYS` (default 90), invalid date format, `end_date < start_date`. Called by every FastAPI endpoint that queries `fact_trade` or `fact_daily_analytics`. Configurable via env var for administrative override.

**GSR-005:** `serving_connection()` opens DuckDB with `read_only=True`. Unlimited concurrent readers; write lock never acquired by serving layer. `pipeline_connection()` acquires write lock only during scheduled pipeline window. Schema DDL separated into `initialize_schema()` called at pipeline start.

**F-023 Schema fix:** `fact_trade` contains only additive tick-level measures (price, volume, value). `fact_daily_analytics` holds all non-additive measures (VWAP, Amihud illiquidity, price_impact_bps) at daily grain — the semantically correct Kimball grain.

---

### F-023 + F-025 Resolution
**File:** `dbt/models/marts/fact_daily_analytics.py`  
**Status:** IMPLEMENTED + REGRESSION-TESTED (non-SARIMA path; SARIMA requires statsmodels)

**F-023:** VWAP = `total_value / total_volume` computed at daily grain via `groupby(["symbol_key", "session_date_key"])`. Amihud illiquidity = `|daily_return| / daily_volume`. Price impact bps = `(high - low) / close × 10000`. None of these measures are stored in `fact_trade` at tick grain.

**F-025:** Per-symbol `try/except` wraps SARIMAX fit. Non-convergent symbols receive `sarima_status = "FAILED_CONVERGENCE"` with `NULL` trend/seasonal components. Symbols with insufficient data receive `"INSUFFICIENT_DATA"`. Missing statsmodels package: `"SKIPPED_NO_STATSMODELS"`. dbt run never blocked by single-symbol SARIMA failure.

---

### F-020 + F-026 Accepted-Risk Record

**F-020 (completeness unverifiable):** Row-count DQ assertion at staging layer (`stg_psx_eod.sql` validates price > 0 and non-negative volume). No expected symbol count is available from PSX to verify completeness. PSX has historically delivered incomplete files — this risk is documented and accepted for a research/portfolio artifact.

**F-026 (FIX feed optional):** `price_impact_bps` is documented as a proxy (high-low spread / close × 10000). Full bid-ask decomposition requires FIX tick feed. The platform name ("Microstructure Analytics") overstates capability without FIX feed. Accepted-risk for EOD-only deployment; FIX integration is a future enhancement.

---

## Regression Validation Record

### Test Execution History (chronological, append-only)

**Run 1 (pre-fix baseline):**
- 6 failures: F-019 manifest tests (same-second timestamp), FileExistsError import, F-022 boundary off-by-one, VWAP assertion wrong value, rebuild test dir missing, manifest query pattern timestamp collision.

**Timestamp fix applied (psx_ingest.py):**
- Second-precision → millisecond-precision in both `ingest_psx_csv()` and `create_computed_version()`.

**Three test assertion fixes applied (test_psx_analytics_regression.py):**
1. Removed `from psx_ingest import FileExistsError` — uses builtin `FileExistsError`.
2. F-022 boundary: `2025-01-01` to `2025-04-01` is 91 days inclusive; corrected to `2025-03-31`.
3. VWAP expected value: corrected from `10.168` to `10.19375` (= 81550 / 8000).

**Run 2 (partial fix):**
- 1 failure: `test_manifest_rebuild_from_raw_selects_latest_modified` — `am2/` subdirectory not created before CSV write. `amended/` had been fixed; `am2/` had not.

**Root cause:** `_make_csv()` helper used by all F-019 tests did not create parent directories. Each test that passed a subdirectory path required a separate `mkdir()` call.

**Final fix applied:** `_make_csv()` now calls `tmp_path.mkdir(parents=True, exist_ok=True)` as its first line. Eliminates the entire class of missing-parent-directory failures for all current and future `_make_csv()` call sites.

**Run 3 (final):**
- **17 passed, 0 failed** (3 deselected: SARIMA tests requiring statsmodels)
- SARIMA tests excluded: `statsmodels` not installed in this environment. SARIMA failure is handled per-symbol in the model; absence of statsmodels does not break the test suite by design (F-025 isolation).

---

## Accepted-Risk and Deferred Items

| Item | Status | Guardrail |
|---|---|---|
| F-020 completeness | ACCEPTED-RISK | Row-count + price-range DQ at staging |
| F-026 FIX feed | ACCEPTED-RISK | `price_impact_bps` documented as proxy |
| SARIMA tests (statsmodels absent) | ENV-CONSTRAINT | 3 tests deselected; production env must have statsmodels pinned |
| DuckDB read-only serving mode in tests | STRUCTURAL | Tests exercise `validate_date_range()` and schema only; DuckDB file-level tests require integration env |