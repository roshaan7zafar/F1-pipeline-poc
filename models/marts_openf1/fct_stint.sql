{{ config(materialized='table') }}

with joined as (
  select
    s.session_key,
    s.driver_number,
    s.stint_number,
    s.compound,
    s.lap_start,
    s.lap_end,
    d.driver_sk
  from {{ ref('stg_stints') }} s
  join {{ ref('dim_driver') }} d
    on d.session_key = s.session_key
   and d.driver_number = s.driver_number
)

select
  {{ dbt_utils.generate_surrogate_key(['session_key','driver_number','stint_number']) }} as stint_pk,
  driver_sk,
  session_key,
  driver_number,
  stint_number,
  compound,
  lap_start,
  lap_end
from joined
