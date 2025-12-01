create or replace table silver_jobs as
select
    substring(province, 1, 2) as zone_id,
    "province" as zone_type,
    branch as economic_branch,
    cast(substring(year, 1, 4) as INTEGER) as year,
    measure as job_measure,
    cast(total * 1000 as INTEGER) as jobs_people
from bronze_ine_empleo