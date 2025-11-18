CREATE OR REPLACE TABLE silver_demographics AS
SELECT
    p.municipality AS municipality_id,
    p.year,
    SUM(TRY_CAST(p.total AS INTEGER)) AS population,
    MAX(TRY_CAST(e.gdp AS DOUBLE)) AS gdp
FROM bronze_ine_poblacion p
LEFT JOIN bronze_ine_pib e
    ON p.province = e.province 
    AND p.year = e.year
WHERE e.branch = 'PRODUCTO INTERIOR BRUTO A PRECIOS DE MERCADO'
GROUP BY p.municipality, p.year