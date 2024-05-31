

{{ config(materialized='table') }}

with bronze as (
    select * from {{ ref('raw_data') }}
)
select 
    CAST(id AS text) as id,
    CAST(name AS text) as name,
    CAST(brewery_type AS text) as brewery_type,
    CAST(address AS text) as address,
    CAST(city AS text) as city,
    CAST(postal_code AS text) as postal_code,
    CAST(country AS text) as country,
    CAST(longitude AS float) as longitude,
    CAST(latitude AS float) as latitude,
    CAST(phone AS text) as phone,
    CAST(website_url AS text) as website_url,
    CAST(state AS text) as state,
    CAST(street AS text) as street,
    CAST(state_province AS text) as state_province
from bronze
where state is not null
