-- models/staging/stg_psx_eod.sql
-- ===========================================================
-- Staging view: PSX EOD data sourced from manifest-canonical Parquet paths.
--
-- F-019 enforcement: This model does NOT glob over raw/symbol/date/*.parquet.
-- Globbing would read ALL files for a (symbol, date) key — including superseded
-- amendment files — and produce duplicate rows silently.
--
-- Correct pattern: read manifest.json, extract canonical raw_path per key,
-- read only that path. DuckDB's read_parquet() accepts a list of paths.
--
-- In dbt+DuckDB, the manifest is pre-resolved by the pipeline DAG task
-- `generate_manifest_source_list` (called before dbt_staging_run) which
-- writes a staging source list to data/staging_source_list.json.
-- This model reads that resolved list — not the manifest directly.
--
-- F-021 enforcement: raw/ file permissions are defense-in-depth only.
-- This model's correctness does not depend on filesystem immutability.
-- The manifest is the authority; even if a raw/ file is physically overwritten,
-- this model reads the path the manifest declares as canonical.
--
-- GSR-005: This model runs inside the pipeline_connection() write window.
-- The serving layer (read_only=True) never touches raw/ directly.

{{ config(
    materialized='view',
    schema='staging',
    tags=['staging', 'psx-eod']
) }}

-- OPTIMIZATION NOTE: Explicit columns over read_json_auto() prevent schema drift exposure
-- and satisfy rule 1 (no SELECT *).
WITH manifest_source AS (
    -- Read the pre-resolved source list written by the pipeline DAG.
    -- Format: [{symbol, session_date, canonical_raw_path, row_count, amended}]
    -- This list contains ONLY canonical paths from manifest.json — never glob paths.
    SELECT
        symbol::VARCHAR               AS symbol,
        session_date::DATE            AS session_date,
        canonical_raw_path::VARCHAR   AS canonical_raw_path,
        row_count::BIGINT             AS row_count,
        amended::BOOLEAN              AS amended
    FROM read_json_auto('{{ env_var("PSX_DATA_ROOT", "/opt/airflow/data") }}/staging_source_list.json')
),

raw_data AS (
    -- DuckDB read_parquet with explicit path list (manifest-derived, not glob).
    -- Each path is the canonical file for its (symbol, session_date) key.
    SELECT
        r.symbol,
        r.session_date::DATE                             AS session_date,
        r.open::DECIMAL(12,4)                           AS open_price,
        r.high::DECIMAL(12,4)                           AS high_price,
        r.low::DECIMAL(12,4)                            AS low_price,
        r.close::DECIMAL(12,4)                          AS close_price,
        r.volume::BIGINT                                AS volume,
        r.value::DECIMAL(18,2)                         AS value,
        r._ingested_at::TIMESTAMPTZ                    AS ingested_at,
        r._source_file                                  AS source_file,
        r._sha256                                       AS sha256,
        m.amended::BOOLEAN                              AS is_amended,
        m.canonical_raw_path
    FROM manifest_source m
    -- Use DuckDB's glob-free read: each row in manifest_source has one canonical_raw_path
    CROSS JOIN read_parquet(m.canonical_raw_path) r
),

validated AS (
    SELECT
        r.*,
        -- F-022 supporting column: session_date is always present for range queries
        r.session_date,
        -- Surrogate key computation: consistent with dim_symbol and dim_session
        -- In production: join to dim_symbol to get symbol_key
        -- In staging view: expose raw symbol code for dbt mart models
        r.symbol AS symbol_code
    FROM raw_data r
    WHERE
        -- Critical DQ: reject zero or negative prices at staging layer
        r.open_price  > 0
        AND r.high_price  > 0
        AND r.low_price   > 0
        AND r.close_price > 0
        -- Reject negative volume
        AND r.volume >= 0
        -- Reject future-dated records (clock skew protection)
        AND r.session_date <= current_date + INTERVAL '1 day'
)

SELECT
    symbol_code,
    session_date,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    value,
    ingested_at,
    source_file,
    sha256,
    is_amended
FROM validated