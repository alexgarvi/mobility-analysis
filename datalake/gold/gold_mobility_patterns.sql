CREATE OR REPLACE TABLE gold_mobility_patterns AS
SELECT
    period,
    CASE 
        WHEN dayofweek(date) IN (0, 6) THEN 'Weekend' 
        ELSE 'Weekday' 
    END AS day_type,
    SUM(travels) AS total_trips,
    approx_quantile(travels, 0.5) AS median_trips_per_hour
FROM silver_trips
GROUP BY 
    period, 
    day_type, 
ORDER BY 
    day_type, period
