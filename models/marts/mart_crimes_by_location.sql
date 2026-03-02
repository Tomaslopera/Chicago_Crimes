{{
    config(
        materialized="table",
        snowflake_warehouse="CRIMENES_CHICAGO",
        schema="MARTS"
    )
}}

select
    f.crime_year,
    l.district,
    l.community_area,
    l.street_direction,
    l.street_type,
    count(*) as total_crimes,
    sum(case when f.arrest then 1 else 0 end) as total_arrests,
    round(avg(case when f.arrest then 1 else 0 end), 4) as arrest_rate,
    sum(case when f.domestic then 1 else 0 end) as domestic_crimes
from {{ ref('fct_crimes') }} f
left join {{ ref('dim_location') }} l using (location_key)
group by 1, 2, 3, 4, 5