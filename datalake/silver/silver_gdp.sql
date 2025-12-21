CREATE OR REPLACE TABLE silver_gdp AS
SELECT
    seccion AS ID,
    fecha AS year,
    total AS gdp_euros
FROM bronze_ine_renta;