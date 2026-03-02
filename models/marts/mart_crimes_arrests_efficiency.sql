{{
    config(
        materialized="table",
        snowflake_warehouse="CRIMENES_CHICAGO",
        schema="MARTS"
    )
}}

select
    crime_type,
    fbi_code,
    crime_year,
    count(*) as total_crimes,
    sum(case when arrest then 1 else 0 end) as total_arrests,
    round(sum(case when arrest then 1 else 0 end)/ nullif(count(*), 0), 4) as arrest_rate,
    rank() over (partition by crime_year order by arrest_rate desc) as arrest_rate_rank
from {{ ref('stg_chicago_crimes') }}
group by 1, 2, 3