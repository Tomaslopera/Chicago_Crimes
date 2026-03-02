{{
    config(
        materialized="view",
        snowflake_warehouse="CRIMENES_CHICAGO",
        schema="STAGING"
    )
}}

with source as (
    select * from {{ source('chicago_raw', 'chicago_crimes') }}
),

cleaned as (
    select
        id as crime_id,
        case_number,
        date as crime_date,
        extract(year from date) as crime_year,
        extract(month from date) as crime_month,
        extract(day from date) as crime_day,
        dayofweek(date) as crime_dayofweek,
        hour(date) as crime_hour,
        updated_on,
        block as block_raw,
        replace(split_part(trim(block), ' ', 1), 'XX', '00') as block_number,
        split_part(trim(block), ' ', 2) as street_direction,
        trim(regexp_replace(
            regexp_replace(trim(block), '^[0-9X]+\\s+[NSEW]+\\s+', ''),
            '\\s+(ST|AVE|DR|BLVD|RD|LN|CT|PL|WAY|PKWY|HWY|EXPY)$', ''
        )) as street_name,
        regexp_substr(trim(block), '(ST|AVE|DR|BLVD|RD|LN|CT|PL|WAY|PKWY|HWY|EXPY)$')as street_type,
        iucr,
        primary_type as crime_type,
        description as crime_description,
        location_description,
        fbi_code,
        arrest,
        domestic,
        beat,
        left(beat, 2) as beat_district_code,
        substr(beat, 3, 1) as beat_sector,
        substr(beat, 4, 1) as beat_number,
        district,
        ward,
        community_area,
        concat('District ', district, ' / Sector ', left(beat, 2), substr(beat, 3, 1)) as police_sector,
        x_coordinate,
        y_coordinate,
        latitude,
        longitude

    from source
    where id is not null
)

select * from cleaned