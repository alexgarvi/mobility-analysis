CREATE TABLE IF NOT EXISTS bronze_mitma_poblacion (
    district VARCHAR,
    municipality VARCHAR,
    province VARCHAR,
    population VARCHAR,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)