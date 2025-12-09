-- models/marts/dim_job_type.sql
with job_type_unique as (
    select distinct
        job_type_std
    from {{ ref('stg_jobs_staging') }}
)
select
    row_number() over (order by job_type_std) as job_type_id,
    job_type_std
from job_type_unique
