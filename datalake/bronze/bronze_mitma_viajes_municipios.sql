CREATE TABLE IF NOT EXISTS bronze_mitma_viajes_municipios (
    date DATE,
    period VARCHAR,
    origin VARCHAR,
    destination VARCHAR,
    distance VARCHAR,
    activity_origin VARCHAR,
    activity_destination VARCHAR,
    study_origin_posible VARCHAR,
    study_destination_posible VARCHAR,
    residence INTEGER,
    rent VARCHAR,
    age VARCHAR,
    gender VARCHAR,
    travels VARCHAR,
    travels_km VARCHAR,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)