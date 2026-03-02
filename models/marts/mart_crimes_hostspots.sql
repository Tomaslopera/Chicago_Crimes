{{
    config(
        materialized="table",
        snowflake_warehouse="CRIMENES_CHICAGO",
        schema="MARTS"
    )
}}

select
    block_raw,
    street_direction,
    street_name,
    street_type,
    district,
    community_area,
    count(*) as total_crimes,
    sum(case when arrest then 1 else 0 end) as total_arrests,
    round(avg(case when arrest then 1 else 0 end), 4) as arrest_rate,
    count(distinct crime_type) as crime_type_variety
from {{ ref('stg_chicago_crimes') }}
group by 1, 2, 3, 4, 5, 6
having total_crimes > 1000
order by total_crimes desc