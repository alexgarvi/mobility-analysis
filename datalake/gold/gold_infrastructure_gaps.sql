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
        pop.population AS origin_pop,
        eco.gdp AS dest_economic_pull
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
    coalesce(
        origin_pop * dest_economic_pull / nullif(pow(euclidean_dist, 2), 0),
        0) AS theoretical_potential, --This way we manage the division by zero by converting to null first and then coalescing to 0
    coalesce(
        actual_flow / nullif(origin_pop * dest_economic_pull / nullif(pow(euclidean_dist, 2), 0), 0),
        0) AS infrastructure_gap_score --This way we manage the division by zero by converting to null first and then coalescing to 0
FROM enriched_flows
WHERE euclidean_dist > 0
ORDER BY infrastructure_gap_score DESC
