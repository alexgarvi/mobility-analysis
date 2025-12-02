CREATE OR REPLACE TABLE silver_gdp AS
SELECT
    SUBSTRING(province, 1, 2) AS zone_id,
    "province" AS zone_type,
    branch AS economic_branch,
    cast(SUBSTRING(year, 1, 4) as INTEGER) AS year,
    COALESCE(CAST(gdp AS DOUBLE), 0) AS gdp_euros
FROM bronze_ine_pib;