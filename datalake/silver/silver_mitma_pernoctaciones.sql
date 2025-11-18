CREATE TABLE IF NOT EXISTS silver_mitma_pernoctaciones (
    date DATE,
    residence_zone VARCHAR,
    overnight_zone VARCHAR,
    num_people DOUBLE,           -- Limpio de 'people' (varchar) a numérico
    zonification_level VARCHAR,  -- Valores: 'DISTRITO', 'MUNICIPIO', 'GAU'
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);