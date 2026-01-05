MERGE INTO bronze_zona_municipios_geometria AS target
USING (source_query) as source
    ON target.municipio_id = source.ID

WHEN MATCHED THEN
    UPDATE SET
        geometria = source.geom

WHEN NOT MATCHED THEN 
    INSERT (
        municipio_id,
        geometria
    )
    VALUES (
        source.ID,
        source.geom
    )