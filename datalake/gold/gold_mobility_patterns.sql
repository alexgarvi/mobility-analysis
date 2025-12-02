CREATE OR REPLACE TABLE gold_mobility_patterns AS
SELECT
    hour_of_day,
    CASE 
        WHEN dayofweek(trip_date) IN (0, 6) THEN 'Weekend' 
        ELSE 'Weekday' 
    END AS day_type,
    activity_origin,
    activity_destination,
    SUM(trips_count) AS total_trips,
    approx_quantiles(trips_count, 0.5) AS median_trips_per_hour
FROM gold_fact_trips
GROUP BY 
    hour_of_day, 
    day_type, 
    activity_origin, 
    activity_destination
ORDER BY 
    day_type, hour_of_day
