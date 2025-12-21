CREATE OR REPLACE TABLE silver_demographics AS
SELECT 
    ipop.seccion as ID,
    ipop.total as population,
    ipop.fecha as year
FROM bronze_ine_poblacion as ipop