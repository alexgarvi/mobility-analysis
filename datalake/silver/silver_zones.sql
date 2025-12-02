CREATE OR REPLACE TABLE silver_zones AS
SELECT
    dc.distrito_id AS zone_id,
    "district" as zone_type,
    dc.longitude,
    dc.latitude,
    dn.name
FROM bronze_zona_distritos_centroides dc
LEFT JOIN bronze_distritos_nombres dn
    ON TRY_CAST(dc.distrito_id AS VARCHAR) = dn.ID

UNION

SELECT
    mc.municipality_id AS zone_id,
    "municipality" as zone_type,
    mc.longitude,
    mc.latitude,
    mn.name
FROM bronze_zona_municipios_centroides mc
LEFT JOIN bronze_municipios_nombres mn
    ON mc.municipality_id = mn.municipality_id

UNION

SELECT
    gc.gau_id AS zone_id,
    "gau" as zone_type,
    gc.longitude,
    gc.latitude,
    gn.name AS name
FROM bronze_zona_gaus_centroides gc
LEFT JOIN bronze_gaus_nombres gn
    ON TRY_CAST(gc.gau_id AS VARCHAR) = gn.ID

UNION

SELECT
    province_id AS zone_id,
    "province" as zone_type,
    NULL AS longitude,
    NULL AS latitude,
    province AS name
FROM bronze_provincias;
