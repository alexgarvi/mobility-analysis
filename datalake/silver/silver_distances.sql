CREATE OR REPLACE TABLE silver_distances AS
SELECT
    A.ID AS zone_a_id,
    B.ID AS zone_b_id,
    'district' AS zone_type,
    NULLIF(
        cast(
            SQRT(
            POWER(CAST(A.lon AS FLOAT) - CAST(b.lon AS FLOAT), 2) + 
            POWER(CAST(A.lat AS FLOAT) - CAST(b.lat AS FLOAT), 2)
            )
        as DOUBLE)
    , 0) AS distance_meters --Using NULLIF to prevent division by zero later on
FROM bronze_zona_distritos_centroides A
JOIN bronze_zona_distritos_centroides B on a.ID < b.ID --To avoid duplicates or calculating both directions

UNION

SELECT
    A.ID AS zone_a_id,
    B.ID AS zone_b_id,
    'municipality' AS zone_type,
    NULLIF(
        cast(
            SQRT(
            POWER(CAST(A.lon AS FLOAT) - CAST(b.lon AS FLOAT), 2) + 
            POWER(CAST(A.lat AS FLOAT) - CAST(b.lat AS FLOAT), 2)
            )
        as DOUBLE)
    , 0) AS distance_meters --Using NULLIF to prevent division by zero later on
FROM bronze_zona_municipios_centroides A
JOIN bronze_zona_municipios_centroides B on a.ID < b.ID --To avoid duplicates or calculating both directions

UNION

SELECT
    A.ID AS zone_a_id,
    B.ID AS zone_b_id,
    'gau' AS zone_type,
    NULLIF(
        cast(
            SQRT(
            POWER(CAST(A.lon AS FLOAT) - CAST(b.lon AS FLOAT), 2) + 
            POWER(CAST(A.lat AS FLOAT) - CAST(b.lat AS FLOAT), 2)
            )
        as DOUBLE)
    , 0) AS distance_meters --Using NULLIF to prevent division by zero later on
FROM bronze_zona_gaus_centroides A
JOIN bronze_zona_gaus_centroides B on a.ID < b.ID --To avoid duplicates or calculating both directions