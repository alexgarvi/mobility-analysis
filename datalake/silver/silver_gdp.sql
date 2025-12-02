CREATE OR REPLACE TABLE silver_gdp AS
SELECT
    SUBSTRING(Provincias, 1, 2) AS zone_id,
    'province' AS zone_type,
    "Ramas de actividad" AS economic_branch,
    cast(SUBSTRING(Periodo, 1, 4) as INTEGER) AS year,
    COALESCE(CAST(REPLACE(Total, '.', '') AS DOUBLE), 0) AS gdp_euros
FROM bronze_ine_pib;