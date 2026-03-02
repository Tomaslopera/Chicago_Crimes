{{
    config(
        materialized="table",
        snowflake_warehouse="CRIMENES_CHICAGO",
        schema="MARTS"
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['iucr']) }} as crime_type_key,
    iucr,
    crime_type,
    crime_description,
    fbi_code
from {{ ref('stg_chicago_crimes') }}
qualify row_number() over (partition by iucr order by crime_date desc) = 1