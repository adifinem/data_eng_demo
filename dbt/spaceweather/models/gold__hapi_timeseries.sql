{{ config(materialized='table') }}

SELECT
  dataset_id,
  scope,
  ts,
  ingest_ts,
  parameter,
  value_num,
  value_raw
FROM {{ ref('silver__hapi_measurements') }}
WHERE parameter NOT IN ('time')
