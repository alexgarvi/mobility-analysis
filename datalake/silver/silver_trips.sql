CREATE OR REPLACE TABLE silver_trips AS 
SELECT
    
    CAST(STRPTIME(vd.date, '%Y%m%d') AS DATE) as date,
    CAST(vd.period AS INTEGER) as period,
    vd.origin as origin_id,
    vd.destination as destination_id,
    SUM(CAST(vd.travels AS DOUBLE)) as travels,   
    SUM(CAST(vd.travels_km AS DOUBLE)) as travels_km

FROM bronze_mitma_viajes_distritos as vd
GROUP BY 
    date,
    period,
    origin_id,
    destination_id