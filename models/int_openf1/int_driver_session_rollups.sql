{{ config(materialized='view') }}

with clean as (
  select session_key, driver_number, lap_time_s
  from {{ ref('int_laps_enriched') }}
  where is_clean_lap
)
select
  session_key,
  driver_number,
  count(*)                      as laps_clean,
  min(lap_time_s)               as best_lap_s,
  median(lap_time_s)            as median_lap_s,
  stddev_samp(lap_time_s)       as lap_stddev_s
from clean
group by 1,2
