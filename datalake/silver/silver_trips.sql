CREATE OR REPLACE TABLE silver_trips AS 
SELECT
    vd.date as date,
    vd.period as period,
    vd.origin as origin_id,
    vd.destination as destination_id,
    SUM(CAST(vd.travels AS DOUBLE)) as travels,   
    SUM(CAST(vd.travels_km AS DOUBLE)) as travels_km

FROM bronze_mitma_viajes_distritos as vd