MERGE INTO bronze_zona_municipios_centroides AS target
USING (source_query) as source
    ON target.municipio_id = source.ID

WHEN MATCHED THEN
    UPDATE SET
        centroide = source.geom

WHEN NOT MATCHED THEN 
    INSERT (
        municipio_id,
        centroide
    )
    VALUES (
        source.ID,
        source.geom
    )