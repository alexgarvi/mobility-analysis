CREATE OR REPLACE TABLE silver_distances AS
SELECT
    A.distrito_id AS zone_a_id,
    B.distrito_id AS zone_b_id,
    ST_DISTANCE_SPHEROID(A.centroide, b.centroide) as distance_meters

FROM bronze_zona_distritos_centroides A
JOIN bronze_zona_distritos_centroides B on a.distrito_id < b.distrito_id