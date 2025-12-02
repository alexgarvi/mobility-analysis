CREATE OR REPLACE TABLE gold_infrastructure_gaps AS
WITH zone_flows AS (
    SELECT 
        pkZoneOrigin, 
        pkZoneDestination, 
        SUM(trips_count) AS actual_flow_trips
    FROM gold_fact_trips
    GROUP BY pkZoneOrigin, pkZoneDestination
),
enriched_flows AS (
    SELECT
        zf.pkZoneOrigin,
        zf.pkZoneDestination,
        zf.actual_flow_trips,
        pop.population_people AS origin_population_people,
        eco.gdp_euros AS dest_gdp_euros
    FROM zone_flows zf
    JOIN gold_dim_zones oz ON zf.pkZoneOrigin = oz.pkZone
    JOIN gold_dim_zones dz ON zf.pkZoneDestination = dz.pkZone
    LEFT JOIN gold_fact_demographics pop ON oz.pkZone = pop.pkZone
    LEFT JOIN gold_fact_gdp eco ON dz.pkZone = eco.pkZone
),
SELECT
    ef.pkZoneOrigin,
    ef.pkZoneDestination,
    ef.actual_flow_trips,
    ef.origin_population_people,
    ef.dest_gdp_euros,
    fd.distance_meters,
    ef.origin_population_people * ef.dest_gdp_euros / pow(fd.distance_meters, 2) AS theoretical_potential,
    ef.actual_flow / ef.origin_population_people * ef.dest_gdp_euros / pow(fd.distance_meters, 2) AS infrastructure_gap_score
FROM enriched_flows ef
LEFT JOIN gold_fact_distances fd ON ef.pkZoneOrigin = fd.pkZoneA AND ef.pkZoneDestination = fd.pkZoneB
ORDER BY infrastructure_gap_score DESC