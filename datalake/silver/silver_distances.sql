CREATE OR REPLACE TABLE silver_distances AS
SELECT
    A.distrito_id AS district_a,
    B.distrito_id AS district_b,
    SQRT(
        POWER(A.longitude - B.longitude, 2) + 
        POWER(A.latitude - B.latitude, 2)
    ) AS distance
FROM
    bronze_zona_distritos_centroides A
CROSS JOIN
    bronze_zona_distritos_centroides B