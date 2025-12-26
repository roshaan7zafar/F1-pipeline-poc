{{ config(materialized='view') }}

SELECT
    session_key::number             as session_key,
    meeting_key::number             as meeting_key,
    session_name::string            as session_name,
    session_type::string            as session_type,
    country_name::string            as country_name,
    location::string                as location,
    circuit_short_name:: string     as circuit_short_name,
    to_timestamp_tz(date_start)     as date_start_utc,
    to_timestamp_tz(date_end)       as date_end_utc,
    gmt_offset::string              as gmt_offset,
    year::number                    as year 

FROM {{ source('raw_openf1','OPENF_1_SESSIONS') }}