{{ config(materialized='view') }}

{% if execute %}
  {% set result = run_query("SELECT COUNT(*) AS cnt FROM glob('/datalake/bronze/hapi/**/*.parquet')") %}
  {% set has_files = (result.rows[0]['cnt'] if result and result.rows else 0) %}
{% else %}
  {% set has_files = 1 %}
{% endif %}

{% if has_files == 0 %}

select
  cast(null as varchar) as dataset_id,
  cast(null as varchar) as scope,
  cast(null as timestamp) as ts,
  cast(null as timestamp) as ingest_ts,
  cast(null as varchar) as parameter,
  cast(null as double) as value_num,
  cast(null as varchar) as value_raw
where 1 = 0

{% else %}

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

{% endif %}
