CREATE TABLE IF NOT EXISTS bronze_ine_empleo (
    province VARCHAR,
    branch VARCHAR,
    measure VARCHAR,
    year INTEGER,
    total FLOAT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)