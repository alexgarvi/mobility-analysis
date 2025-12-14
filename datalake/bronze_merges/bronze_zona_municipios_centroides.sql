MERGE INTO bronze_zona_municipios_centroides AS target
USING source_query as source
    ON target.municipio_id = source.municipio_id

WHEN MATCHED THEN
    UPDATE SET
        target.longitude = source.longitude,
        target.latitude = source.latitude

WHEN NOT MATCHED THEN 
    INSERT (
        municipio_id,
        longitude,
        latitude
    )
    VALUES (
        source.municipio_id,
        longitude,
        source.latitude
    )