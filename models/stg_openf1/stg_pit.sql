{{ config(materialized='view') }}

select
  session_key::number    as session_key,
  driver_number::number  as driver_number,
  lap_number::number     as lap_number,
  try_to_timestamp_ntz(date) as pit_time_utc,
  try_to_decimal(pit_duration,9,3) as pit_duration_s
from {{ source('raw_openf1','OPENF_1_PIT') }}
