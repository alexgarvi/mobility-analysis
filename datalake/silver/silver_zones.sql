CREATE OR REPLACE TABLE silver_zones AS
SELECT
    dc.ID AS zone_id,
    'district' as zone_type,
    dc.lon,
    dc.lat,
    dn.name
FROM bronze_zona_distritos_centroides dc
LEFT JOIN bronze_distritos_nombres dn
    ON TRY_CAST(dc.ID AS VARCHAR) = dn.ID

UNION

SELECT
    mc.ID AS zone_id,
    'municipality' as zone_type,
    mc.lon,
    mc.lat,
    mn.column2
FROM bronze_zona_municipios_centroides mc
LEFT JOIN bronze_municipios_nombres mn
    ON mc.ID = mn.column1

UNION

SELECT
    gc.ID AS zone_id,
    'gau' as zone_type,
    gc.lon,
    gc.lat,
    gn.name AS name
FROM bronze_zona_gaus_centroides gc
LEFT JOIN bronze_gaus_nombres gn
    ON TRY_CAST(gc.ID AS VARCHAR) = gn.ID

UNION

SELECT
    province_id AS zone_id,
    'province' as zone_type,
    NULL AS longitude,
    NULL AS latitude,
    province AS name
FROM bronze_provincias;
