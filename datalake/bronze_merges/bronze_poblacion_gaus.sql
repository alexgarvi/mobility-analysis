MERGE INTO bronze_poblacion_gaus AS target
USING source_query as source
    ON target.ID = source.'01001'

WHEN MATCHED THEN
    UPDATE SET
        target.population = source.'2925.0'

WHEN NOT MATCHED THEN 
    INSERT (
        ID,
        population
    )
    VALUES (
        source.'01001',
        source.'2925.0'
    )