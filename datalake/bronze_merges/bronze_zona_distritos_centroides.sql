MERGE INTO bronze_zona_distritos_centroides AS target
USING source_query as source
    ON target.distrito_id = source.distrito_id

WHEN MATCHED THEN
    UPDATE SET
        target.longitude = source.longitude,
        target.latitude = source.latitude

WHEN NOT MATCHED THEN 
    INSERT (
        distrito_id,
        longitude,
        latitude
    )
    VALUES (
        source.distrito_id,
        longitude,
        source.latitude
    )