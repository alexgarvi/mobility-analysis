CREATE OR REPLACE TABLE silver_trips AS 
SELECT
    CAST(vd.date AS DATE) as date,
    CAST(vd.period AS INTEGER) as period,
    vd.origin as origin_id,
    vd.destination as destination_id,
    SUM(CAST(vd.travels AS DOUBLE)) as travels,   
    SUM(CAST(vd.travels_km AS DOUBLE)) as travels_km

FROM bronze_mitma_viajes_distritos as vd