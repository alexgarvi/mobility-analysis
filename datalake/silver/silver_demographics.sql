CREATE OR REPLACE TABLE silver_demographics AS
SELECT
    column0 as zone_id,
    'district' as zone_type,
    NULL as gender,
    NULL as age,
    NULL as nationality,
    NULL as year,
    COALESCE(TRY_CAST(column1 as INTEGER), 0) as population_people
FROM bronze_poblacion_distritos

UNION

SELECT
    SUBSTRING(Municipios, 1, 2) as zone_id,
    'municipality' as zone_type,
    Sexo,
    Edad,
    Nacionalidad,
    cast(Periodo as INTEGER) as year,
    COALESCE(SUM(TRY_CAST(REPLACE(Total, '.', '') as INTEGER)), 0) as population_people
FROM bronze_ine_poblacion
GROUP BY
    SUBSTRING(Municipios, 1, 2),
    Sexo,
    Edad,
    Nacionalidad,
    Periodo

UNION

SELECT
    column0 as zone_id,
    'gau' as zone_type,
    NULL as gender,
    NULL as age,
    NULL as nationality,
    NULL as year,
    COALESCE(TRY_CAST(column1 as INTEGER), 0) as population_people
FROM bronze_poblacion_gaus

UNION

SELECT
    SUBSTRING(Provincias, 1, 2) as zone_id,
    'province' as zone_type,
    Sexo,
    Edad,
    Nacionalidad,
    cast(Periodo as INTEGER) as year,
    COALESCE(SUM(TRY_CAST(REPLACE(Total, '.', '') as INTEGER)), 0) as population_people
FROM bronze_ine_poblacion
GROUP BY
    SUBSTRING(Provincias, 1, 2),
    Sexo,
    Edad,
    Nacionalidad,
    Periodo;