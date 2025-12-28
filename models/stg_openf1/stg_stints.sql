{{ config(materialized='view') }}

select
  session_key::number    as session_key,
  driver_number::number  as driver_number,
  stint_number::number   as stint_number,
  compound::string       as compound,
  lap_start::number      as lap_start,
  lap_end::number        as lap_end
from {{ source('raw_openf1','OPENF_1_STINTS') }}
