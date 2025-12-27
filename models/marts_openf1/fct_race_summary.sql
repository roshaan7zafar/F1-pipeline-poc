{{ config(materialized='table') }}

with race_sessions as (
  select session_key, meeting_key, year
  from {{ ref('stg_sessions') }}
  where lower(session_type) = 'race'
),
pace as (
  -- driver-session lap pace rollups (already filtered to clean laps upstream)
  select session_key, driver_number, laps_clean, best_lap_s, median_lap_s, lap_stddev_s
  from {{ ref('int_driver_session_rollups') }}
),
pit as (
  -- pit aggregates per driver-session
  select
    session_key,
    driver_number,
    count(*)                              as pit_stops,
    avg(pit_duration_s)                   as avg_pit_s,
    min(pit_duration_s)                   as best_pit_s
  from {{ ref('stg_pit') }}
  group by 1,2
),
stints as (
  -- tire strategy per driver-session
  select
    session_key,
    driver_number,
    count(*) as stint_count,
    listagg(distinct coalesce(compound,'UNKNOWN'), ', ')
      within group (order by compound)   as compounds_used
  from {{ ref('stg_stints') }}
  group by 1,2
),
drivers as (
  select
    {{ dbt_utils.generate_surrogate_key(['session_key','driver_number']) }} as driver_sk,
    session_key, driver_number, full_name, name_acronym, team_name, team_colour
  from {{ ref('dim_driver') }}
)
select
  d.driver_sk,
  rs.meeting_key,
  rs.session_key,
  d.driver_number,
  d.full_name,
  d.name_acronym,
  d.team_name,
  d.team_colour,

  -- pace
  p.laps_clean,
  p.best_lap_s,
  p.median_lap_s,
  p.lap_stddev_s,

  -- pits
  coalesce(pt.pit_stops, 0)  as pit_stops,
  pt.avg_pit_s,
  pt.best_pit_s,

  -- tires
  coalesce(st.stint_count, 0) as stint_count,
  st.compounds_used,

  rs.year
from race_sessions rs
join pace p
  on p.session_key = rs.session_key
join drivers d
  on d.session_key = p.session_key and d.driver_number = p.driver_number
left join pit pt
  on pt.session_key = rs.session_key and pt.driver_number = d.driver_number
left join stints st
  on st.session_key = rs.session_key and st.driver_number = d.driver_number;
