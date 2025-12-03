CREATE OR REPLACE TABLE silver_festivos AS

SELECT CAST(STRPTIME(fecha, '%Y-%m-%d') AS DATE) AS date,
CAST(bf.es_festivo AS INTEGER) = 1 AS is_holiday,
CAST(bf.es_fin_de_semana AS INTEGER) = 1 AS is_weekend
FROM bronze_festivos as bf