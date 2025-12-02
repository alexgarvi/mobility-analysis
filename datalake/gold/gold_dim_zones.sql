CREATE OR REPLACE TABLE gold_dim_zones AS
SELECT
    zone_id,
    zone_type,
    CONCAT(zone_type, '|', zone_id) AS pkZone,
    longitude,
    latitude,
    name
FROM silver_zones;