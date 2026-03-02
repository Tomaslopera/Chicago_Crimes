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
    crime_dayofweek,
    crime_hour,
    count(*) as total_crimes,
    sum(case when arrest then 1 else 0 end) as total_arrests,
    round(avg(case when arrest then 1 else 0 end), 4) as arrest_rate
from {{ ref('fct_crimes') }}
group by 1, 2, 3, 4