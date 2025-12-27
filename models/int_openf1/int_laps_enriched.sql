{{ config(materialized='view') }}

with laps as (
  select * from {{ ref('stg_laps') }}
),
drivers as (
  select session_key, driver_number, full_name, name_acronym, team_name, team_color
  from {{ ref('stg_drivers') }}
)
select
  l.session_key,
  l.driver_number,
  l.lap_number,
  l.lap_time_s,
  l.is_pit_out_lap,
  -- "clean lap" definition: timed and not pit-out (tweak as you like)
  case when l.lap_time_s is not null and coalesce(l.is_pit_out_lap,false)=false
       then true else false end as is_clean_lap,
  d.full_name, d.name_acronym, d.team_name, d.team_colour
from laps l
left join drivers d
  on d.session_key = l.session_key and d.driver_number = l.driver_number
