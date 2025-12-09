-- models/marts/dim_salary.sql
with salary_unique as (
    select distinct
        salary_offered,
        salary_min,
        salary_max,
        salary_currency,
        salary_parsed
    from {{ ref('stg_jobs_staging') }}
)
select
    row_number() over (order by salary_offered) as salary_id,
    salary_offered,
    salary_min,
    salary_max,
    salary_currency,
    salary_parsed
from salary_unique
