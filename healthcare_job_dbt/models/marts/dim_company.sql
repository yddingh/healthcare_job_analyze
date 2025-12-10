{{ config(materialized='table') }}
with company_unique as (
    select distinct
        company_name,
        city,
        country
    from {{ ref('stg_jobs_staging') }}
)
select
    row_number() over (order by company_name) as company_id,
    company_name,
    city,
    country
from company_unique
