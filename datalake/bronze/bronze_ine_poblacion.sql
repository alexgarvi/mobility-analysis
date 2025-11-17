CREATE TABLE IF NOT EXISTS bronze_ine_poblacion (
    province VARCHAR,
    municipality VARCHAR,
    gender VARCHAR,
    age VARCHAR,
    nationality VARCHAR,
    year VARCHAR,
    total INTEGER,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)