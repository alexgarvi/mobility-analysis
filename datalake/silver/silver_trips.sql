CREATE OR REPLACE TABLE silver_trips AS
SELECT
    CAST(STRPTIME(fecha, '%Y%m%d') AS DATE) AS trip_date,
    TRY_CAST(periodo AS INTEGER) AS hour_of_day,
    'district' AS zone_type,
    SUBSTRING(Origen, 1, 2) AS origin_zone_id,
    SUBSTRING(Destino, 1, 2) AS destination_zone_id,
    actividad_origen AS activity_origin,
    actividad_destino AS activity_destination,
    SUM(CAST(viajes AS DOUBLE)) AS trips_count,
    SUM(CAST(viajes_km AS DOUBLE)) AS trips_km
FROM bronze_mitma_viajes_distritos
GROUP BY trip_date, hour_of_day, zone_type, origin_zone_id, destination_zone_id, activity_origin, activity_destination

UNION

SELECT
    CAST(STRPTIME(fecha, '%Y%m%d') AS DATE) AS trip_date,
    TRY_CAST(periodo AS INTEGER) AS hour_of_day,
    'municipality' AS zone_type,
    origen AS origin_zone_id,
    destino AS destination_zone_id,
    actividad_origen AS activity_origin,
    actividad_destino AS activity_destination,
    SUM(CAST(viajes AS DOUBLE)) AS trips_count,
    SUM(CAST(viajes_km AS DOUBLE)) AS trips_km
FROM bronze_mitma_viajes_municipios
GROUP BY trip_date, hour_of_day, zone_type, origin_zone_id, destination_zone_id, activity_origin, activity_destination

UNION

SELECT
    CAST(STRPTIME(fecha, '%Y%m%d') AS DATE) AS trip_date,
    TRY_CAST(periodo AS INTEGER) AS hour_of_day,
    'gau' AS zone_type,
    SUBSTRING(origen, 1, 2) AS origin_zone_id,
    SUBSTRING(destino, 1, 2) AS destination_zone_id,
    actividad_origen AS activity_origin,
    actividad_destino AS activity_destination,
    SUM(CAST(viajes AS DOUBLE)) AS trips_count,
    SUM(CAST(viajes_km AS DOUBLE)) AS trips_km
FROM bronze_mitma_viajes_gau
GROUP BY trip_date, hour_of_day, zone_type, origin_zone_id, destination_zone_id, activity_origin, activity_destination