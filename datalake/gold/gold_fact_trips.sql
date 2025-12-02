CREATE OR REPLACE TABLE gold_fact_trips AS
SELECT
    trip_date,
    hour_of_day,
    zone_type,
    origin_zone_id,
    destination_zone_id,
    CONCAT(zone_type, '|', origin_zone_id) AS pkZoneOrigin,
    CONCAT(zone_type, '|', destination_zone_id) AS pkZoneDestination,
    activity_origin,
    activity_destination,
    trips_count,
    trips_km
FROM silver_trips