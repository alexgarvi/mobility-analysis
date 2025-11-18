CREATE TABLE IF NOT EXISTS silver_mitma_poblacion (
    district_id VARCHAR,
    municipality_id VARCHAR,
    province_id VARCHAR,
    population_total DOUBLE,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);