CREATE OR REPLACE TABLE gold_fact_distances AS
SELECT
    zone_a_id,
    zone_b_id,
    zone_type,
    CONCAT(zone_type, '|', zone_a_id) AS pkZoneA,
    CONCAT(zone_type, '|', zone_b_id) AS pkZoneB,
    distance_meters
FROM silver_distances;