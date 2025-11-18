CREATE TABLE IF NOT EXISTS silver_metricas_economia (
    province_id VARCHAR,
    year INTEGER,
    economic_branch VARCHAR,     
    gdp_value DOUBLE,            
    employment_total DOUBLE,     
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);