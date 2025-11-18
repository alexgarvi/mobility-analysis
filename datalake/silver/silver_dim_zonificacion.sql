CREATE TABLE IF NOT EXISTS silver_dim_zonificacion (
    mitma_district_id VARCHAR,
    mitma_municipality_id VARCHAR,
    mitma_gau_id VARCHAR,
    ine_municipality_id VARCHAR, 
    centroid_longitude DOUBLE,   
    centroid_latitude DOUBLE,
    zone_level VARCHAR,          
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);