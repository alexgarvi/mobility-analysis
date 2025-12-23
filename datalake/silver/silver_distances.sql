CREATE OR REPLACE TABLE silver_distances AS
SELECT
    A.ID AS zone_a_id,
    B.ID AS zone_b_id,
    ST_DISTANCE_SPHEROID(A.centroid, b.centroid) as distance_meters

FROM bronze_zona_distritos_centroides A
JOIN bronze_zona_distritos_centroides B on a.ID < b.ID