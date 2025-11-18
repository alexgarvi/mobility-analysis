CREATE TABLE IF NOT EXISTS silver_distancia_zonas (
    origin_zone VARCHAR,
    destination_zone VARCHAR,
    distance_km DOUBLE,   
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);