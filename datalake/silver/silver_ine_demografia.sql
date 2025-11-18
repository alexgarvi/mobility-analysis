CREATE TABLE IF NOT EXISTS silver_ine_demografia (
    province_name VARCHAR,
    municipality_name VARCHAR,
    gender VARCHAR,
    age_group VARCHAR,
    nationality VARCHAR,
    year INTEGER,
    total_population INTEGER,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);