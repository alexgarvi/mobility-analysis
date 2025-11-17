CREATE TABLE IF NOT EXISTS bronze_ine_pib (
    province VARCHAR,
    branch VARCHAR,
    year VARCHAR,
    gdp FLOAT,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)