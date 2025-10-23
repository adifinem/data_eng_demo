
  
    
    

    create  table
      "spaceweather"."main"."gold__hapi_timeseries__dbt_tmp"
  
    as (
      

SELECT
  dataset_id,
  scope,
  ts,
  ingest_ts,
  parameter,
  value_num,
  value_raw
FROM "spaceweather"."main"."silver__hapi_measurements"
WHERE parameter NOT IN ('time')
    );
  
  