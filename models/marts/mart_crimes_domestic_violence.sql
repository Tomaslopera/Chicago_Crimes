{{
    config(
        materialized="table",
        snowflake_warehouse="CRIMENES_CHICAGO",
        schema="MARTS"
    )
}}

select
    crime_year,
    crime_month,
    district,
    community_area,
    crime_type,
    count(*) as total_domestic_crimes,
    sum(case when arrest then 1 else 0 end) as arrests_made,
    round(avg(case when arrest then 1 else 0 end), 4) as arrest_rate
from {{ ref('stg_chicago_crimes') }}
where domestic = true
group by 1, 2, 3, 4, 5