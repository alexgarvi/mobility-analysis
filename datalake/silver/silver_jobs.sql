create or replace table silver_jobs as
select
    substring(Provincias, 1, 2) as zone_id,
    'province' as zone_type,
    "Ramas de actividad" as economic_branch,
    cast(substring(Periodo, 1, 4) as INTEGER) as year,
    Magnitud as job_measure,
    CAST(CAST(REPLACE(REPLACE(Total, '.', ''), ',', '.') AS FLOAT) * 1000 as INTEGER) as jobs_people
    -- muy poco legible, basicamente cambia el formato de europa a eeuu (1.234,5 -> 1234.5)
from bronze_ine_empleo