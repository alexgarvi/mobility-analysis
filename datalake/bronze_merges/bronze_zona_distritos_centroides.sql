MERGE INTO bronze_zona_distritos_centroides AS target
USING (source_query) as source
    ON target.distrito_id = source.ID

WHEN MATCHED THEN
    UPDATE SET
        centroide = source.geom

WHEN NOT MATCHED THEN 
    INSERT (
        distrito_id,
        centroide
    )
    VALUES (
        source.ID,
        source.geom
    )