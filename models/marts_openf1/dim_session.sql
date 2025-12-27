{{ config(materialized='table') }}

select
  session_key,
  meeting_key,
  session_name, session_type,
  country_name, location, circuit_short_name,
  date_start_utc, date_end_utc, gmt_offset, year
from {{ ref('stg_sessions') }}
