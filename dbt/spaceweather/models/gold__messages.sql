{{ config(materialized='table') }}

-- Pull the latest bronze records into DuckDB
WITH src AS (
  SELECT *
  FROM read_parquet('/datalake/bronze/healthcheck/*/*.parquet')
)
SELECT
  ts,
  source,
  ingest_ts,
  dataset_id,
  dt,
  -- flatten a couple of health fields if present; fallback to NULL
  try_cast(data['status'] AS VARCHAR)     AS api_status,
  try_cast(data['detail'] AS VARCHAR)     AS api_detail
FROM src
