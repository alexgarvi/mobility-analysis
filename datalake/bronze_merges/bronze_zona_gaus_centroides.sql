MERGE INTO bronze_zona_gaus_centroides AS target
USING (source_query) as source
    ON target.gaus_id = source.ID

WHEN MATCHED THEN
    UPDATE SET
        centroide = source.geom

WHEN NOT MATCHED THEN 
    INSERT (
        gaus_id,
        centroide
    )
    VALUES (
        source.ID,
        source.geom
    )