{{
    config(
        materialized="table",
        snowflake_warehouse="CRIMENES_CHICAGO",
        schema="MARTS"
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['block_raw', 'district', 'ward', 'community_area']) }} as location_key,
    block_raw,
    block_number,
    street_direction,
    street_name,
    street_type,
    beat,
    district,
    ward,
    community_area,
    location_description,
    latitude,
    longitude
from {{ ref('stg_chicago_crimes') }}
where latitude is not null
qualify row_number() over (partition by block_raw, district, ward, community_area order by crime_date desc) = 1