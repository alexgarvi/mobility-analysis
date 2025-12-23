MERGE INTO bronze_municipios_nombres AS target
USING source_query as source
    ON target.ID = source.column1

WHEN MATCHED THEN
    UPDATE SET
        target.name = source.column2

WHEN NOT MATCHED THEN 
    INSERT (
        ID,
        name
    )
    VALUES (
        source.column1,
        source.column2
    )