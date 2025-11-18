CREATE TABLE IF NOT EXISTS silver_mitma_viajes (
    date DATE,
    period_hour INTEGER,    
    origin_zone VARCHAR,
    destination_zone VARCHAR,
    distance_range VARCHAR, 
    activity_origin VARCHAR,
    activity_destination VARCHAR,
    residence_province VARCHAR,
    age_group VARCHAR,
    gender VARCHAR,
    num_travels DOUBLE,     
    travels_km DOUBLE,           
    zonification_level VARCHAR,  
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);