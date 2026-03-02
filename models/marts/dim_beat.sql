{{
    config(
        materialized="table",
        snowflake_warehouse="CRIMENES_CHICAGO",
        schema="MARTS"
    )
}}

select
    beat,
    beat_district_code,
    beat_sector,
    beat_number,
    district,
    police_sector,
    count(*) as total_crimes,
    sum(case when arrest then 1 else 0 end) as total_arrests,
    round(avg(case when arrest then 1 else 0 end), 4) as arrest_rate
from {{ ref('stg_chicago_crimes') }}
group by 1, 2, 3, 4, 5, 6