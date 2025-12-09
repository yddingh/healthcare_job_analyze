select
    j.job_id,
    j.post_date_parsed,
    c.category_id,
    t.job_type_id,
    j.job_title,
    j.job_description_clean,
    j.desc_word_count,
    j.skill_flags,
    co.company_id,
    s.salary_id
from {{ ref('stg_jobs_staging') }} j

-- join category
left join {{ ref('dim_category') }} c
    on j.category = c.category

-- join job_type
left join {{ ref('dim_job_type') }} t
    on j.job_type_std = t.job_type_std

-- join company
left join {{ ref('dim_company') }} co
    on j.company_name = co.company_name
    and j.city = co.city
    and j.country = co.country

-- join salary
left join {{ ref('dim_salary') }} s
    on j.salary_offered = s.salary_offered
    and j.salary_min = s.salary_min
    and j.salary_max = s.salary_max
    and j.salary_currency = s.salary_currency
    and j.salary_parsed = s.salary_parsed
