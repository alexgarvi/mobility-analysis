MERGE INTO bronze_gaus_nombres AS target 
USING source_query AS source
    ON target.fecha = source.fecha

WHEN MATCHED THEN 
    UPDATE SET
        target.es_festivo = source.es_festivo,
        target.es_fin_de_semana = source.es_fin_de_semana

WHEN NOT MATCHED THEN 
    INSERT (
        fecha,
        es_festivo,
        es_fin_de_semana
    )
    VALUES (
        source.fecha,
        source.es_festivo,
        source.es_fin_de_semana
    )
