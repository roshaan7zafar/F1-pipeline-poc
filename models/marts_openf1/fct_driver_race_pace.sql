{{ config(materialized='table') }}

select
  d.driver_sk,
  r.session_key,
  r.driver_number,
  r.laps_clean,
  r.best_lap_s,
  r.median_lap_s,
  r.lap_stddev_s
from {{ ref('int_driver_session_rollups') }} r
join {{ ref('dim_driver') }} d
  on d.session_key = r.session_key and d.driver_number = r.driver_number
