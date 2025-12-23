MERGE INTO bronze_zona_gaus_geometria AS target
USING source_query as source
    ON target.gaus_id = source.ID

WHEN MATCHED THEN
    UPDATE SET
        target.geometria = source.geom

WHEN NOT MATCHED THEN 
    INSERT (
        gaus_id,
        geometria
    )
    VALUES (
        source.ID,
        source.geom
    )