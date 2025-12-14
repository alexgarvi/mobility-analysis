MERGE INTO bronze_municipios_nombres AS target
USING source_query as source
    ON target.ID = source.ID

WHEN MATCHED THEN
    UPDATE SET
        target.name = source.name

WHEN NOT MATCHED THEN 
    INSERT (
        ID,
        name
    )
    VALUES (
        source.ID,
        source.name
    )