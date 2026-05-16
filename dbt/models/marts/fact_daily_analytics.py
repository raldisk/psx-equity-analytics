-- models/marts/fact_daily_analytics.py (dbt Python model)
-- ===========================================================
-- P3 F-025 Fix: SARIMA non-convergence isolation.
--
-- Prior architecture: SARIMA fit ran inside a dbt Python model with no
-- exception handling. Non-convergent SARIMA (common for low-liquidity PSX
-- symbols) raised an exception that blocked the entire dbt run.
--
-- Fix: SARIMA fit is wrapped in a per-symbol try/except. On failure:
--   - sarima_status = 'FAILED_CONVERGENCE'
--   - trend_component = NULL
--   - seasonal_component = NULL
-- The dbt model succeeds for all symbols; analysts see the status flag.
--
-- F-023 Fix: VWAP and Amihud are computed at daily grain here — not stored
-- at raw tick grain. This is the correct Kimball grain for these measures.
-- Queries doing SUM(vwap) across days are semantically wrong and should use
-- volume-weighted averages; the mart schema documents this via column comments.
--
-- Dependency chain:
--   stg_psx_eod (staging view) → fact_daily_analytics (this model)
--   corporate_action_log → (triggers computed/ versioning via Airflow)

import logging
import numpy as np

log = logging.getLogger(__name__)


def model(dbt, session):
    """
    dbt Python model: compute daily analytics for all symbols.
    Returns a DataFrame matching the fact_daily_analytics schema.
    """
    # Read staging data
    stg = dbt.ref("stg_psx_eod")
    df  = stg.df()

    if df.empty:
        log.warning("stg_psx_eod is empty — returning empty fact_daily_analytics")
        return df.iloc[0:0]  # Empty DataFrame with correct schema

    # ── Compute additive daily aggregations ─────────────────────────────────
    daily = df.groupby(["symbol_key", "session_date_key"]).agg(
        open_price  =("price", "first"),
        high_price  =("price", "max"),
        low_price   =("price", "min"),
        close_price =("price", "last"),
        total_volume=("volume", "sum"),
        total_value =("value", "sum"),
        trade_count =("price", "count"),
    ).reset_index()

    # ── Compute non-additive measures at correct daily grain (F-023) ─────────
    # VWAP = total_value / total_volume (defined at daily grain)
    daily["vwap"] = (daily["total_value"] / daily["total_volume"]).round(4)
    daily["vwap"] = daily["vwap"].where(daily["total_volume"] > 0, other=None)

    # Amihud illiquidity = |daily_return| / daily_volume
    # Requires prior close — join on lagged close per symbol
    daily = daily.sort_values(["symbol_key", "session_date_key"])
    daily["prior_close"] = daily.groupby("symbol_key")["close_price"].shift(1)
    daily["daily_return"] = abs(
        (daily["close_price"] - daily["prior_close"]) / daily["prior_close"]
    )
    daily["amihud_illiquidity"] = (
        daily["daily_return"] / daily["total_volume"].replace(0, None)
    ).round(8)

    # Price impact bps — proxy without FIX feed: high-low spread / close
    # NOTED: accurate bid-ask spread decomposition requires FIX feed (F-026)
    daily["price_impact_bps"] = (
        (daily["high_price"] - daily["low_price"]) / daily["close_price"] * 10000
    ).round(4)

    # ── F-025: SARIMA per-symbol with convergence isolation ───────────────────
    try:
        from statsmodels.tsa.statespace.sarimax import SARIMAX
        _statsmodels_available = True
    except ImportError:
        log.warning(
            "statsmodels not installed — SARIMA skipped for all symbols. "
            "Set sarima_status=SKIPPED_NO_STATSMODELS."
        )
        _statsmodels_available = False

    trend_results = []
    for symbol_key in daily["symbol_key"].unique():
        sym_data = daily[daily["symbol_key"] == symbol_key].sort_values("session_date_key")
        close_series = sym_data["close_price"].values

        status = "PENDING"
        trend = np.full(len(close_series), np.nan)
        seasonal = np.full(len(close_series), np.nan)

        if not _statsmodels_available:
            status = "SKIPPED_NO_STATSMODELS"
        elif len(close_series) < 10:
            # Too few observations for SARIMA — not a convergence failure
            status = "INSUFFICIENT_DATA"
            log.debug("Symbol %d: insufficient data (%d rows) for SARIMA", symbol_key, len(close_series))
        else:
            try:
                # F-025 Fix: catch ALL exceptions, including convergence failures.
                # Non-convergent SARIMA raises np.linalg.LinAlgError or ConvergenceWarning
                # promoted to error. Both are caught here without blocking the model run.
                model_fit = SARIMAX(
                    close_series,
                    order=(1, 1, 1),
                    seasonal_order=(1, 1, 1, 5),  # 5-day weekly seasonality
                    enforce_stationarity=False,
                    enforce_invertibility=False,
                ).fit(
                    disp=False,
                    maxiter=50,         # Bound iterations to prevent infinite loops
                    method="lbfgs",
                )
                # Decompose into trend and residual
                trend[:] = model_fit.fittedvalues
                status = "OK"
                log.debug("Symbol %d: SARIMA converged", symbol_key)
            except Exception as e:
                # F-025: isolated failure — log and mark, do NOT re-raise
                status = "FAILED_CONVERGENCE"
                log.warning(
                    "Symbol %d: SARIMA failed to converge (%s: %s). "
                    "trend_component=NULL, seasonal_component=NULL. "
                    "dbt run continues for remaining symbols.",
                    symbol_key, type(e).__name__, str(e)[:80]
                )

        sym_rows = sym_data.copy()
        sym_rows["trend_component"]    = trend
        sym_rows["seasonal_component"] = seasonal
        sym_rows["sarima_status"]      = status
        trend_results.append(sym_rows)

    # ── Assemble final DataFrame ───────────────────────────────────────────────
    if trend_results:
        import pandas as pd
        result = pd.concat(trend_results, ignore_index=True)
    else:
        result = daily.copy()
        result["trend_component"]    = None
        result["seasonal_component"] = None
        result["sarima_status"]      = "PENDING"

    # Add computed version and timestamp
    result["computed_version"] = 0  # Overridden by corporate action pipeline
    result["_computed_at"]     = str(dbt.config.get("invocation_id", ""))

    # Drop internal columns not in target schema
    result = result.drop(columns=["prior_close", "daily_return"], errors="ignore")

    # Report SARIMA outcomes
    if "sarima_status" in result.columns:
        status_counts = result.drop_duplicates("symbol_key")["sarima_status"].value_counts().to_dict()
        log.info("SARIMA outcomes per symbol: %s", status_counts)
        failed = status_counts.get("FAILED_CONVERGENCE", 0)
        if failed > 0:
            log.warning(
                "%d symbol(s) had SARIMA convergence failure. "
                "These symbols have NULL trend_component but are included in the mart. "
                "Analysts should filter on sarima_status='OK' for trend-dependent analysis.",
                failed
            )

    return result