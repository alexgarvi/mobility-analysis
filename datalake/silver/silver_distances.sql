CREATE OR REPLACE TABLE silver_distances AS
SELECT
    A.distrito_id AS zone_a_id,
    B.distrito_id AS zone_b_id,
    "district" AS zone_type,
    NULLIF(
        cast(
            SQRT(
            POWER(A.longitude - B.longitude, 2) + 
            POWER(A.latitude - B.latitude, 2)
            )
        as DOUBLE)
    , 0) AS distance_meters --Using NULLIF to prevent division by zero later on
FROM bronze_zona_distritos_centroides A
JOIN bronze_zona_distritos_centroides B on a.distrito_id < b.distrito_id --To avoid duplicates or calculating both directions

UNION

SELECT
    A.municipio_id AS zone_a_id,
    B.municipio_id AS zone_b_id,
    "municipality" AS zone_type,
    NULLIF(
        cast(
            SQRT(
            POWER(A.longitude - B.longitude, 2) + 
            POWER(A.latitude - B.latitude, 2)
            )
        as DOUBLE)
    , 0) AS distance_meters --Using NULLIF to prevent division by zero later on
FROM bronze_zona_municipios_centroides A
JOIN bronze_zona_municipios_centroides B on a.municipio_id < b.municipio_id --To avoid duplicates or calculating both directions

UNION

SELECT
    A.gaus_id AS zone_a_id,
    B.gaus_id AS zone_b_id,
    "gau" AS zone_type,
    NULLIF(
        cast(
            SQRT(
            POWER(A.longitude - B.longitude, 2) + 
            POWER(A.latitude - B.latitude, 2)
            )
        as DOUBLE)
    , 0) AS distance_meters --Using NULLIF to prevent division by zero later on
FROM bronze_zona_gaus_centroides A
JOIN bronze_zona_gaus_centroides B on a.gau_id < b.gau_id --To avoid duplicates or calculating both directions