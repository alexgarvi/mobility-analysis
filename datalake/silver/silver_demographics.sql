CREATE OR REPLACE TABLE silver_demographics AS
SELECT 
    ipop.seccion AS ID,
    CAST(ipop.total AS INTEGER) as population,
    CAST(ipop.fecha AS DATE) as year
FROM bronze_ine_poblacion as ipop