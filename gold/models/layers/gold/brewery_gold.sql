-- models/gold/brewery_gold.sql
{{ config(materialized='view') }}


with silver as (
    select 
        id,
        name,
        brewery_type,
        city,
        state,
        country,
    from {{ ref('brewery_silver') }}
)
select
    state,
    brewery_type,
    count(id) as brewery_count
from silver
group by state, brewery_type
