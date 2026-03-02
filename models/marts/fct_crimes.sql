{{
    config(
        materialized="table",
        snowflake_warehouse="CRIMENES_CHICAGO",
        schema="MARTS"
    )
}}

select
    c.crime_id,
    c.case_number,
    c.crime_date,
    c.crime_year,
    c.crime_month,
    c.crime_day,
    c.crime_dayofweek,
    c.crime_hour,
    ct.crime_type_key,
    l.location_key,
    c.arrest,
    c.domestic,
    c.updated_on
    from {{ ref('stg_chicago_crimes') }} c
    left join {{ ref('dim_crime_type') }} ct on c.iucr = ct.iucr
    left join {{ ref('dim_location') }} l
    on c.block_raw = l.block_raw
    and c.district = l.district
    and c.ward = l.ward
    and c.community_area = l.community_area