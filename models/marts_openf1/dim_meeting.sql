{{ config(materialized='table') }}

with s as (
  select *
  from {{ ref('stg_sessions') }}
),
agg as (
  select
    meeting_key,
    any_value(year)                as year,
    any_value(country_name)        as country_name,
    any_value(location)            as location,
    any_value(circuit_short_name)  as circuit_short_name,
    min(date_start_utc)            as weekend_start_utc,
    max(date_end_utc)              as weekend_end_utc,
    count(*)                       as sessions_count,
    listagg(distinct session_name, ', ')
      within group (order by session_name) as sessions_in_weekend
  from s
  group by meeting_key
),
race as (
  -- pick the (latest) race session_key for this meeting, if any
  select meeting_key, session_key as race_session_key
  from (
    select
      meeting_key, session_key, date_start_utc,
      row_number() over (partition by meeting_key order by date_start_utc desc) as rn
    from s
    where lower(session_type) = 'race'
  )
  where rn = 1
)
select
  a.*,
  r.race_session_key
from agg a
left join race r using (meeting_key)
