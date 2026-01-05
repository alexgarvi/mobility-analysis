MERGE INTO bronze_ine_renta AS target
USING (source_query) as source
    ON target.seccion = source.Codigo
    AND target.fecha = source.Fecha

WHEN MATCHED THEN
    UPDATE SET
        total = source.Valor

WHEN NOT MATCHED THEN 
    INSERT (
        seccion,
        fecha,
        total
    )
    VALUES (
        source.Codigo,
        source.Fecha,
        source.Valor
    )
