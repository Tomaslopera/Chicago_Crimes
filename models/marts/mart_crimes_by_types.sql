{{
    config(
        materialized="table",
        snowflake_warehouse="CRIMENES_CHICAGO",
        schema="MARTS"
    )
}}

select
    f.crime_year,
    f.crime_month,
    ct.crime_type,
    ct.fbi_code,
    count(*) as total_crimes,
    sum(case when f.arrest then 1 else 0 end) as total_arrests,
    round(avg(case when f.arrest then 1 else 0 end), 4) as arrest_rate
from {{ ref('fct_crimes') }} f
left join {{ ref('dim_crime_type') }} ct using (crime_type_key)
group by 1, 2, 3, 4