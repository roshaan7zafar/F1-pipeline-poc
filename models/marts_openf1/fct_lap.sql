{{ config(materialized='table') }}

select
  {{ dbt_utils.generate_surrogate_key(['l.session_key','l.driver_number','l.lap_number']) }} as lap_pk,
  d.driver_sk,
  l.session_key, l.driver_number, l.lap_number, l.lap_time_s, l.is_pit_out_lap
from {{ ref('int_laps_enriched') }} l
join {{ ref('dim_driver') }} d
  on d.session_key = l.session_key and d.driver_number = l.driver_number
