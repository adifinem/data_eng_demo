


  
  




WITH raw AS (
  SELECT
    dataset_id,
    scope,
    event_time AS ts,
    ingest_ts,
    data_json
  FROM read_parquet('/datalake/bronze/hapi/**/*.parquet', hive_partitioning=TRUE)
),
flattened AS (
  SELECT
    dataset_id,
    scope,
    ts,
    ingest_ts,
    j.key AS parameter,
    j.value AS value_raw
  FROM raw,
  LATERAL json_each(raw.data_json) AS j
)
SELECT
  dataset_id,
  scope,
  ts,
  ingest_ts,
  parameter,
  CASE
    WHEN try_cast(value_raw AS DOUBLE) IS NULL THEN NULL
    WHEN try_cast(value_raw AS DOUBLE) <= -1e29 THEN NULL
    ELSE try_cast(value_raw AS DOUBLE)
  END AS value_num,
  value_raw
FROM flattened
WHERE parameter NOT IN ('dataset_id', 'scope', 'ingest_ts')

