CREATE OR REPLACE TABLE gold_fact_gdp AS
SELECT
    zone_id,
    zone_type,
    CONCAT(zone_type, '|', zone_id) AS pkZone,
    economic_branch,
    year,
    gdp_euros
FROM silver_gdp;