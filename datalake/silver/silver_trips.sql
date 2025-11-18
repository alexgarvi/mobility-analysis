CREATE OR REPLACE TABLE silver_trips AS
SELECT
    CAST(STRPTIME(v.date, '%Y%m%d') AS DATE) AS trip_date,
    TRY_CAST(v.period AS INTEGER) AS hour_of_day,
    v.origin AS origin_zone_id,
    v.destination AS destination_zone_id,
    p1.province_id AS origin_province_id,
    --p1.province AS origin_province,
    p2.province_id AS destination_province_id,
    --p2.province AS destination_province,
    v.activity_origin,
    v.activity_destination,
    TRY_CAST(REPLACE(v.travels, ',', '.') AS DOUBLE) AS trips_count,
    TRY_CAST(REPLACE(v.travels_km, ',', '.') AS DOUBLE) AS trips_km,
    d.distance
FROM bronze_mitma_viajes_municipios v
LEFT JOIN bronze_provincias p1
    ON SUBSTRING(v.origin, 1, 2) = p1.province_id 
LEFT JOIN bronze_provincias p2
    ON SUBSTRING(v.destination, 1, 2) = p2.province_id
LEFT JOIN silver_distances d
    ON SUBSTRING(v.origin, 1, 5) = CAST(district_a AS VARCHAR) AND SUBSTRING(v.destination, 1, 5) = CAST(district_b AS VARCHAR)
