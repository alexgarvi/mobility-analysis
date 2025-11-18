CREATE TABLE IF NOT EXISTS silver_mitma_personas (
    date DATE,
    zone_id VARCHAR,
    age_group VARCHAR,
    gender VARCHAR,
    trips_per_person DOUBLE,
    zonification_level VARCHAR,  
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);