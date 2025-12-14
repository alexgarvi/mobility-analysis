MERGE INTO bronze_zona_gaus_centroides AS target
USING source_query as source
    ON target.gaus_id = source.gaus_id

WHEN MATCHED THEN
    UPDATE SET
        target.longitude = source.longitude,
        target.latitude = source.latitude

WHEN NOT MATCHED THEN 
    INSERT (
        gaus_id,
        longitude,
        latitude
    )
    VALUES (
        source.gaus_id,
        longitude,
        source.latitude
    )