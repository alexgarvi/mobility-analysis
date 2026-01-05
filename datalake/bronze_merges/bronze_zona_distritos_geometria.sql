MERGE INTO bronze_zona_distritos_geometria AS target
USING (source_query) as source
    ON target.distrito_id = source.ID

WHEN MATCHED THEN
    UPDATE SET
        geometria = source.geom

WHEN NOT MATCHED THEN 
    INSERT (
        distrito_id,
        geometria
    )
    VALUES (
        source.ID,
        source.geom
    )