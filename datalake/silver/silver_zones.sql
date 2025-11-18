CREATE OR REPLACE TABLE silver_zones AS
SELECT DISTINCT
    z.mitma_district AS zone_id,
    z.ine_municipality AS municipality_id,
    z.mitma_gau AS gau_id,
    c.longitude,
    c.latitude,
    m.name,
    p.province_id,
    p.province
FROM bronze_mitma_relacion_ine_zonificacion z
LEFT JOIN bronze_zona_distritos_centroides c
    ON TRY_CAST(z.mitma_district AS INTEGER) = c.distrito_id
LEFT JOIN bronze_municipios_nombres m
    ON z.ine_municipality = m.municipality_id
LEFT JOIN bronze_provincias p
    ON SUBSTRING(z.ine_municipality, 1, 2) = p.province_id
