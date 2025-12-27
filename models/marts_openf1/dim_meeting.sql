{{ config(materialized='table') }}

with s as (
  select * from {{ ref('stg_sessions') }}
),

-- weekend-level aggregates (one row per meeting_key)
agg as (
  select
    meeting_key,
    min(year)                as year,
    min(country_name)        as country_name,
    min(location)            as location,
    min(circuit_short_name)  as circuit_short_name,
    min(date_start_utc)      as weekend_start_utc,
    max(date_end_utc)        as weekend_end_utc,
    count(*)                 as sessions_count
  from s
  group by meeting_key
),

-- build distinct session_name list per meeting, then listagg (no DISTINCT inside LISTAGG)
session_names_distinct as (
  select distinct meeting_key, session_name
  from s
),
names_agg as (
  select
    meeting_key,
    listagg(session_name, ', ') within group (order by session_name) as sessions_in_weekend
  from session_names_distinct
  group by meeting_key
),

-- choose the latest race session per meeting (if any)
race as (
  select meeting_key, session_key as race_session_key
  from (
    select
      meeting_key,
      session_key,
      date_start_utc,
      row_number() over (partition by meeting_key order by date_start_utc desc) as rn
    from s
    where lower(session_type) = 'race'
  )
  where rn = 1
)

select
  a.meeting_key,
  a.year,
  a.country_name,
  a.location,
  a.circuit_short_name,
  a.weekend_start_utc,
  a.weekend_end_utc,
  a.sessions_count,
  n.sessions_in_weekend,
  r.race_session_key
from agg a
left join names_agg n
  on a.meeting_key = n.meeting_key
left join race r
  on a.meeting_key = r.meeting_key;
