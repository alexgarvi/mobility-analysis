CREATE OR REPLACE TABLE silver_trips AS
SELECT
    CAST(STRPTIME(date, '%Y%m%d') AS DATE) AS trip_date,
    TRY_CAST(period AS INTEGER) AS hour_of_day,
    "district" AS zone_type,
    SUBSTRING(origin, 1, 2) AS origin_zone_id,
    SUBSTRING(destination, 1, 2) AS destination_zone_id,
    activity_origin,
    activity_destination,
    SUM(TRY_CAST(REPLACE(travels, ',', '.') AS DOUBLE)) AS trips_count,
    SUM(TRY_CAST(REPLACE(travels_km, ',', '.') AS DOUBLE)) AS trips_km
FROM bronze_mitma_viajes_distritos

UNION

SELECT
    CAST(STRPTIME(date, '%Y%m%d') AS DATE) AS trip_date,
    TRY_CAST(period AS INTEGER) AS hour_of_day,
    "municipality" AS zone_type,
    origin AS origin_zone_id,
    destination AS destination_zone_id,
    activity_origin,
    activity_destination,
    TRY_CAST(REPLACE(travels, ',', '.') AS DOUBLE) AS trips_count,
    TRY_CAST(REPLACE(travels_km, ',', '.') AS DOUBLE) AS trips_km
FROM bronze_mitma_viajes_municipios

UNION

SELECT
    CAST(STRPTIME(date, '%Y%m%d') AS DATE) AS trip_date,
    TRY_CAST(period AS INTEGER) AS hour_of_day,
    "gau" AS zone_type,
    SUBSTRING(origin, 1, 2) AS origin_zone_id,
    SUBSTRING(destination, 1, 2) AS destination_zone_id,
    activity_origin,
    activity_destination,
    SUM(TRY_CAST(REPLACE(travels, ',', '.') AS DOUBLE)) AS trips_count,
    SUM(TRY_CAST(REPLACE(travels_km, ',', '.') AS DOUBLE)) AS trips_km
FROM bronze_mitma_viajes_gau