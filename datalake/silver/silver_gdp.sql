CREATE OR REPLACE TABLE silver_gdp AS
SELECT
    seccion AS ID,
    year(CAST(fecha AS DATE)) AS year,
    CAST(total AS DOUBLE) AS gdp_euros
FROM bronze_ine_renta;