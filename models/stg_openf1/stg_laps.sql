{{ config(materialized='view') }}

WITH base AS (
    SELECT
        session_key::number                     as session_key,
        driver_number::number                   as driver_number,
        lap_number::number                      as lap_number,
        try_to_decimal(duration_sector_1,9,3)   as s1_s,
        try_to_decimal(duration_sector_2,9,3)   as s2_s,
        try_to_decimal(duration_sector_3,9,3)   as s3_s,
        try_to_decimal(lap_duration,9,3)        as lap_time_s_raw,
        is_pit_out_lap::boolean                 as is_pit_out_lap
    FROM {{ source('raw_openf1','OPENF_1_LAPS') }}
    
)

SELECT
    * ,
    COALESCE(lap_time_s_raw,
            case when s1_s is not null and s2_s is not null and s3_s is not null
            then s1_s + s2_s + s3_s end) as lap_time_s
FROM base

-- select
--   *,
--   coalesce(
--     lap_time_s_raw,
--     case
--       when s1_s is not null and s2_s is not null and s3_s is not null
--         then s1_s + s2_s + s3_s
--     end
--   ) as lap_time_s
-- from base;