MERGE INTO bronze_mitma_viajes_distritos AS target
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
        travels = source.viajes,
        travels_km = source.viajes_km

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
    source.fecha,
    source.periodo, 
    source.origen, 
    source.destino, 
    source.distancia, 
    source.actividad_origen, 
    source.actividad_destino, 
    source.estudio_origen_posible, 
    source.estudio_destino_posible, 
    source.residencia, 
    source.renta, 
    source.edad, 
    source.sexo,
    source.viajes,
    source.viajes_km);