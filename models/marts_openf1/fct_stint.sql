{{ config(materialized='table') }}

select
  {{ dbt_utils.generate_surrogate_key(['session_key','driver_number','stint_number']) }} as stint_pk,
  d.driver_sk,
  s.session_key, s.driver_number, s.stint_number, s.compound, s.lap_start, s.lap_end
from {{ ref('stg_stints') }} s
join {{ ref('dim_driver') }} d
  on d.session_key = s.session_key and d.driver_number = s.driver_number
