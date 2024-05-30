

{{ config(materialized='table') }}

with bronze as (
    select * from {{ ref('raw_data') }}
)
select 
    id AS text,
    name AS text,
    brewery_type AS text,
    address AS text,
    city AS text,
    postal_code AS text,
    state AS text,
    country AS text,
    longitude AS float,
    latitude AS float,
    phone AS bigint,
    website_url AS text,
    state AS text,
    street AS text,
    state_province AS text
from bronze
where state is not null
