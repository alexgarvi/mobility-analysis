CREATE OR REPLACE TABLE silver_demographics AS
SELECT
    p.province as province_id,
    p.municipality AS municipality_id,
    p.year,
    SUM(TRY_CAST(p.population AS INTEGER)) AS population,
    MAX(TRY_CAST(REPLACE(e.gdp, '.', '') AS DOUBLE)) AS gdp,
    j.jobs
FROM (
    SELECT DISTINCT SUBSTRING(province, 1, 2) as province, SUBSTRING(municipality, 1, 5) as municipality, year, SUM(total) as population
    FROM bronze_ine_poblacion
    WHERE year = '2023'
    GROUP BY province, municipality, year
) p
LEFT JOIN bronze_ine_pib e
    ON p.province = SUBSTRING(e.province, 1, 2)
LEFT JOIN (
    SELECT DISTINCT SUBSTRING(province, 1, 2) as province, TRY_CAST(REPLACE(total, ',', '.') AS FLOAT) as jobs
    FROM bronze_ine_empleo
    WHERE measure = 'Empleo total (miles de personas)'
        AND branch = 'TOTAL PERSONAS'
) j
    ON j.province = p.province
WHERE e.branch = 'PRODUCTO INTERIOR BRUTO A PRECIOS DE MERCADO'
GROUP BY p.province, p.municipality, p.year, j.jobs