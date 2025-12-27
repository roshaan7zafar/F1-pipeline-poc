{{ config(materialized='table') }}

with p as (
  select * from {{ ref('int_pit_enriched') }}
)
select
  {{ dbt_utils.generate_surrogate_key(
    ['session_key','driver_number','lap_number',"to_char(pit_time_utc,'YYYY-MM-DD\"T\"HH24:MI:SS.FF3')"]
  ) }} as pit_pk,
  d.driver_sk,
  p.session_key, p.driver_number, p.lap_number, p.pit_time_utc, p.pit_duration_s, p.stop_seq
from p
join {{ ref('dim_driver') }} d
  on d.session_key = p.session_key and d.driver_number = p.driver_number
