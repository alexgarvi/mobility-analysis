CREATE OR REPLACE TABLE gold_fact_demographics AS
SELECT
    zone_id,
    zone_type,
    CONCAT(zone_type, '|', zone_id) AS pkZone,
    gender,
    age,
    year,
    population_people
FROM silver_demographics;