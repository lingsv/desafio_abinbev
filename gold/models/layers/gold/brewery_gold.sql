{{ config(materialized='view') }}

with silver as (
    select * from {{ ref('brewery_silver') }}
)
select 
    brewery_type,
    state,
    country,
    count(*) as brewery_count
from silver
group by brewery_type, state, country