CREATE TABLE IF NOT EXISTS bronze_mitma_personas_distritos (
    date DATE,
    zone VARCHAR,
    age VARCHAR,
    gender VARCHAR,
    travels VARCHAR,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)