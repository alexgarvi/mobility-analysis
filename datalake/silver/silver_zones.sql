CREATE OR REPLACE TABLE silver_zones AS
SELECT 
    dg.distrito_id as ID,
    CAST(dg.geometria AS GEOMETRY) as geometry,
    CAST(dc.centroide AS GEOMETRY) as centroid

FROM bronze_zona_distritos_geometria dg
JOIN bronze_zona_distritos_centroides dc on dg.distrito_id = dc.distrito_id