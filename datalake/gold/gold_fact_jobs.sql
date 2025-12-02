CREATE OR REPLACE TABLE gold_fact_jobs AS
select
    zone_id,
    zone_type,
    CONCAT(zone_type, '|', zone_id) AS pkZone,
    economic_branch,
    year,
    job_measure,
    jobs_people
from silver_jobs;