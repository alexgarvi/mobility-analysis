MERGE INTO bronze_mitma_viajes_municipios AS target
USING (source_query) AS source
ON target.date = source.fecha
    AND target.period = source.periodo 
    AND target.origin = source.origen 
    AND target.destination = source.destino  
    AND target.distance = source.distancia  
    AND target.activity_origin = source.actividad_origen  
    AND target.activity_destination = source.actividad_destino  
    AND target.study_origin_posible = source.estudio_origen_posible 
    AND target.study_destination_posible = source.estudio_destino_posible 
    AND target.residence = source.residencia 
    AND target.rent = source.renta 
    AND target.age = source.edad 
    AND target.gender = source.sexo 

WHEN MATCHED THEN
    UPDATE SET
        target.travels = source.viajes 
        target.travels_km = source.viajes_km

WHEN NOT MATCHED THEN 
    INSERT (
    date,
    period,
    origin,
    destination,
    distance,
    activity_origin,
    activity_destination,
    study_origin_posible,
    study_destination_posible,
    residence,
    rent,
    age,
    gender,
    travels,
    travels_km
    )

    VALUES (
    source.date,
    source.period,
    source.origin,
    source.destination,
    source.distance,
    source.activity_origin,
    source.activity_destination,
    source.study_origin_posible,
    source.study_destination_posible,
    source.residence,
    source.rent,
    source.age,
    source.gender,
    source.travels,
    source.travels_km);