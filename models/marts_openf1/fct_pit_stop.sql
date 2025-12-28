{{ config(materialized='table') }}

with joined as (
  select
    -- disambiguate BEFORE calling the macro
    p.session_key      as session_key_p,
    p.driver_number    as driver_number_p,
    p.lap_number       as lap_number,
    -- stable string for timestamp key
    to_char(p.pit_time_utc, 'YYYY-MM-DD"T"HH24:MI:SS.FF3') as pit_time_key,
    p.pit_time_utc,
    p.pit_duration_s,
    p.stop_seq,
    d.driver_sk
  from {{ ref('int_pit_enriched') }} p
  join {{ ref('dim_driver') }} d
    on d.session_key   = p.session_key
   and d.driver_number = p.driver_number
)

select
  {{ dbt_utils.generate_surrogate_key(['session_key_p','driver_number_p','lap_number','pit_time_key']) }} as pit_pk,
  driver_sk,
  -- expose canonical names in the fact
  session_key_p   as session_key,
  driver_number_p as driver_number,
  lap_number,
  pit_time_utc,
  pit_duration_s,
  stop_seq
from joined
