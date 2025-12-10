{{ config(materialized='table') }}
select *
from stg_jobs
