CREATE OR REPLACE TABLE gold_infrastructure_gaps AS
WITH zone_flows AS (
    SELECT 
        origin_zone_id, 
        destination_zone_id, 
        SUM(trips_count) AS actual_flow
    FROM silver_trips
    GROUP BY origin_zone_id, destination_zone_id
),
enriched_flows AS (
    SELECT
        zf.origin_zone_id,
        zf.destination_zone_id,
        zf.actual_flow,
        oz.longitude AS o_lon, oz.latitude AS o_lat,
        dz.longitude AS d_lon, dz.latitude AS d_lat,
        COALESCE(pop.population, 1000) AS origin_pop,
        COALESCE(eco.gdp, 100000) AS dest_economic_pull
    FROM zone_flows zf
    JOIN silver_zones oz ON zf.origin_zone_id = oz.zone_id
    JOIN silver_zones dz ON zf.destination_zone_id = dz.zone_id
    LEFT JOIN silver_demographics pop ON oz.municipality_id = pop.municipality_id
    LEFT JOIN silver_demographics eco ON dz.municipality_id = eco.municipality_id
)
SELECT
    origin_zone_id,
    destination_zone_id,
    actual_flow,
    origin_pop,
    dest_economic_pull,
    sqrt(pow(o_lon - d_lon, 2) + pow(o_lat - d_lat, 2)) AS euclidean_dist,
    (origin_pop * dest_economic_pull) / (pow(euclidean_dist, 2) + 0.0001) AS theoretical_potential,
    actual_flow / ((origin_pop * dest_economic_pull) / (pow(euclidean_dist, 2) + 0.0001)) AS infrastructure_gap_score
FROM enriched_flows
WHERE euclidean_dist > 0
ORDER BY infrastructure_gap_score DESC
