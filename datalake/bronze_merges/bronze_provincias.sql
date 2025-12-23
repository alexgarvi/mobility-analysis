MERGE INTO bronze_provincias AS target
USING source_query as source
    ON target.ID = source.province_id

WHEN MATCHED THEN
    UPDATE SET
        target.name = source.province

WHEN NOT MATCHED THEN 
    INSERT (
        ID,
        name
    )
    VALUES (
        source.province_id,
        source.province
    )