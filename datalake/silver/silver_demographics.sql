CREATE OR REPLACE TABLE silver_demographics AS
SELECT
    ID as zone_id,
    "district" as zone_type,
    NULL as gender,
    NULL as age,
    NULL as nationality
    NULL as year,
    cast(population as INTEGER) as population_people
FROM bronze_poblacion_distritos

UNION

SELECT
    SUBSTRING(municipality, 1, 2) as zone_id,
    "municipality" as zone_type,
    gender,
    age,
    nationality,
    cast(year as INTEGER) as year,
    cast(sum(total) as INTEGER) as population_people
FROM bronze_ine_poblacion
GROUP BY
    SUBSTRING(municipality, 1, 2),
    gender,
    age,
    nationality,
    year

UNION

SELECT
    ID as zone_id,
    "gau" as zone_type,
    NULL as gender,
    NULL as age,
    NULL as nationality,
    NULL as year,
    cast(population as INTEGER) as population_people
FROM bronze_poblacion_gaus

UNION

SELECT
    SUBSTRING(province, 1, 2) as zone_id,
    "province" as zone_type,
    gender,
    age,
    nationality,
    cast(year as INTEGER) as year,
    cast(sum(total) as INTEGER) as population_people
FROM bronze_ine_poblacion
GROUP BY
    SUBSTRING(province, 1, 2),
    gender,
    age,
    nationality,
    year;