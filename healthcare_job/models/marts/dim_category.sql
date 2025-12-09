-- models/marts/dim_category.sql
with category_unique as (
    select distinct
        category
    from {{ ref('stg_jobs_staging') }}
    where category is not null
)
select
    row_number() over (order by category) as category_id,
    category
from category_unique
