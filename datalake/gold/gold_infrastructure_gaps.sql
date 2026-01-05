CREATE OR REPLACE TABLE gold_infrastructure_gaps AS
WITH model_inputs AS (
    SELECT 
        t.origin_id,
        t.destination_id,

        SUM(t.travels) as actual_trips,

        ds.distance_meters as distance_meters,

        MAX(pop.population) as P_i,
        MAX(econ.gdp_euros) as E_j

    FROM silver_trips t
    JOIN silver_zones zo ON t.origin_id = zo.ID
    JOIN silver_zones zd ON t.destination_id = zd.ID
    JOIN silver_demographics pop ON t.origin_id = pop.ID
    JOIN silver_gdp econ ON t.destination_id = econ.ID
    JOIN silver_distances ds ON t.origin_id = ds.zone_a_id AND t.destination_id = ds.zone_b_id

    WHERE t.date BETWEEN '2023-01-01' AND '2023-12-31'
    GROUP BY origin_id, destination_id, ds.distance_meters
),

calibration_stats AS (
    SELECT 
        SUM(actual_trips) as total_actual,
        SUM( (P_i * E_j) / (CASE WHEN distance_meters < 0.1 THEN 0.1 ELSE distance_meters END ^ 2) ) as total_gravity_raw
    FROM model_inputs
)

SELECT 
    m.origin_id,
    m.destination_id,
    m.actual_trips,

    ( (SELECT total_actual / total_gravity_raw FROM calibration_stats) * (m.P_i * m.E_j) / 
      (CASE WHEN m.distance_meters < 0.1 THEN 0.1 ELSE m.distance_meters END ^ 2) 
    ) AS potential_demand,

    m.actual_trips / NULLIF(
        ( (SELECT total_actual / total_gravity_raw FROM calibration_stats) * (m.P_i * m.E_j) / (CASE WHEN m.distance_meters < 0.1 THEN 0.1 ELSE m.distance_meters END ^ 2) )
    , 0) as gap_ratio

FROM model_inputs m;