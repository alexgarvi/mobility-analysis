from airflow import Dataset
from airflow.sdk import dag, task
from pendulum import datetime
import duckdb
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.hooks.base import BaseHook 
import boto3
from botocore.exceptions import ClientError
from datetime import date, timedelta

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

pg = BaseHook.get_connection("neon_postgres")
aws = BaseHook.get_connection("aws_default")
bucket = "s3://carlos-s3-bdet-ducklake" 

def secreto(con):
    print("Iniciando")
    #con = duckdb.connect( )
    con.sql("INSTALL ducklake; LOAD ducklake;")
    con.sql("INSTALL spatial; LOAD spatial;")
    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql("INSTALL postgres; LOAD postgres;")

    print("Librerias cargadas")

    con.sql(f"""
        CREATE OR REPLACE SECRET secreto_s3 (
        TYPE s3,
        KEY_ID '{aws.login}',
        SECRET '{aws.password}',
        REGION 'eu-central-1'
    )
    """)

    print("Secreto s3 creado")

    con.sql(f"""
        CREATE OR REPLACE SECRET secreto_postgres (
        TYPE postgres,
        HOST '{pg.host}',
        PORT {pg.port},
        DATABASE '{pg.schema}',
        USER '{pg.login}',
        PASSWORD '{pg.password}'
        )
    """)

    print("Secreto postgres creado")

    con.sql("""
        CREATE OR REPLACE SECRET secreto_ducklake (
            TYPE ducklake,
            METADATA_PATH '',
            METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'secreto_postgres'}
        );
        """)
    
    print("secreto ducklake creado")

    con.sql(f"""
        ATTACH 'ducklake:secreto_ducklake' AS mobility_ducklake (DATA_PATH '{bucket}', OVERRIDE_DATA_PATH TRUE) """)
    con.sql("""
        USE mobility_ducklake """)
    
    print("fin")


def build_config(vcpu, memoryGB, query):
    config = {
        "resourceRequirements": [
            {"type": "VCPU", "value": str(vcpu)},
            {"type": "MEMORY", "value": str(int(1024*memoryGB*0.99))}],
        "environment": [
            {"name": "SQL_QUERY", "value": query},
            {"name": "memory", "value": f"{int(memoryGB*0.8)}GB"},
            {"name": "AWS_DEFAULT_REGION", "value": "eu-central-1"},
            {"name": "USUARIO_POSTGRES", "value": "neondb_owner"},
            {"name": "CONTR_POSTGRES", "value": pg.password},
            {"name": "HOST_POSTGRES", "value": pg.host},
            {"name": "RUTA_S3_DUCKLAKE", "value": bucket}]
                            }
    return config


@dag(
    dag_id="dag_gold",
    start_date=datetime(2026, 1, 5),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    max_active_tasks=32,
    tags=["gold-transform"],
    params = {"year": 2023,
              "date_start": (2023, 5, 8),
              "date_end": (2023, 5, 14),
              "polygon": "POLYGON((40.9 -1.7, 40.9 0.9, 37.8 0.9, 37.8 -1.7, 40.9 -1.7))"}
)


#########################################################################################################

# Beginning of DAG

#########################################################################################################


def gold_dag():

    @task()
    def gold_features_config(**context):
        date_start = date(*context["params"]["date_start"])
        date_end = date(*context["params"]["date_end"])
        region = context["params"]["polygon"]

        query = f"""--sql
CREATE OR REPLACE TABLE gold_gravity_features AS

WITH trips_2023 AS (
    -- Agregación masiva inicial
    SELECT 
        origin_id, 
        destination_id, 
        SUM(travels) as actual_trips
    FROM silver_trips
    WHERE date BETWEEN '{date_start}' AND '{date_end}'
    GROUP BY 1, 2
)

SELECT 
    t.origin_id,
    t.destination_id,
    t.actual_trips,
    
    -- Distancia segura (min 500m)
    GREATEST(d.distance_meters, 500.0) as dist_meters,
    
    -- Variables del modelo (Casteamos a DOUBLE para evitar overflow en multiplicaciones)
    CAST(pop.poblacion AS DOUBLE) as P_i,
    CAST(econ.renta_total_euros AS DOUBLE) as E_j,
    
    -- Gravedad Cruda pre-calculada: (P * E) / d^2
    (CAST(pop.poblacion AS DOUBLE) * CAST(econ.renta_total_euros AS DOUBLE)) 
        / POWER(GREATEST(d.distance_meters, 500.0), 2) as gravity_raw

FROM trips_2023 t

-- JOINs con las tablas Silver
JOIN silver_distances d 
    ON t.origin_id = d.origin_id 
    AND t.destination_id = d.destination_id

JOIN silver_demographics pop 
    ON t.origin_id = pop.distrito_id 
    AND pop.year = 2023

JOIN silver_demographics econ 
    ON t.destination_id = econ.distrito_id 
    AND econ.year = 2023;
"""

        con = duckdb.connect()
        secreto(con)
        con.sql(query)

        vcpu = 8
        memoryGB = 16
        return build_config(vcpu, memoryGB, query)


    def gold_gaps(**context):
        date_start = date(*context["params"]["date_start"])
        date_end = date(*context["params"]["date_end"])
        region = context["params"]["polygon"]

        query = f"""--sql
CREATE OR REPLACE TABLE gold_infrastructure_gaps AS

WITH ranked_data AS (
    -- 1. Detectamos outliers sobre los datos ya preparados
    SELECT 
        *,
        -- Ranking basado en rendimiento (Viajes reales vs Gravedad teórica)
        PERCENT_RANK() OVER (ORDER BY actual_trips / NULLIF(gravity_raw, 0)) as percentile_rank
    FROM gold_gravity_features
),

clean_data AS (
    -- 2. Nos quedamos con el núcleo representativo (Quitamos el 5% inferior y superior)
    SELECT * FROM ranked_data
    WHERE percentile_rank BETWEEN 0.05 AND 0.95
),

calibration AS (
    -- 3. Calculamos la constante K global usando solo datos limpios
    SELECT 
        SUM(actual_trips) / SUM(gravity_raw) as k_factor
    FROM clean_data
)

-- 4. Generamos el reporte final
SELECT 
    c.origin_id,
    c.destination_id,
    
    -- Métricas Reales
    c.actual_trips,
    CAST(c.dist_meters AS INTEGER) as distance_meters,
    
    -- Demanda Potencial ( El Modelo )
    CAST(
        (SELECT k_factor FROM calibration) * c.gravity_raw 
    AS INTEGER) as potential_demand,
    
    -- GAP Ratio (Indicador de Negocio)
    -- Si es < 1: Hay menos viajes de los que la economía predice (¿Falta transporte?)
    ROUND(
        c.actual_trips / NULLIF((SELECT k_factor FROM calibration) * c.gravity_raw, 0)
    , 4) as gap_ratio

FROM clean_data c
ORDER BY gap_ratio ASC;
"""
        con = duckdb.connect()
        secreto(con)
        con.sql(query)
        

    @task()
    def temporary_gravity_features_config(**context):
        date_start = date(*context["params"]["date_start"])
        date_end = date(*context["params"]["date_end"])
        current_date = pd.to_datetime(date_start)
        final_date = pd.to_datetime(date_end)
        configs = []

        con = duckdb.connect()
        secreto(con)
        con.sql("""
                    CREATE OR REPLACE TABLE temp_trips_batch_agg (
                        origin_id VARCHAR, 
                        destination_id VARCHAR, 
                        partial_trips BIGINT
                    )""")

        while current_date <= final_date:
            # A. Calcular el final del mes actual
            # Truco: Ir al día 1 del mes siguiente y restar un día
            next_week = (current_date + pd.DateOffset(days=7))
            end_of_current_week = next_week - pd.Timedelta(days=1)
            
            # B. Definir el final del lote (Batch End)
            # Si el final del mes está ANTES que la fecha final global, cortamos en fin de mes.
            # Si no, cortamos en la fecha final global.
            batch_end_date = min(end_of_current_week, final_date)
            
            # C. Formatear para SQL
            s_str = current_date.strftime('%Y-%m-%d')
            e_str = batch_end_date.strftime('%Y-%m-%d')
            
            #print(f" >> Procesando lote: {s_str} al {e_str}")
            
            # D. Ejecutar Agregación Parcial
            query = f"""
            INSERT INTO temp_trips_batch_agg
            SELECT 
                origin_id, 
                destination_id, 
                SUM(travels) as partial_trips
            FROM silver_trips
            WHERE date BETWEEN '{s_str}' AND '{e_str}'
            GROUP BY 1, 2
            """
            #con.sql(query_insert)
            #print(query_insert)
            
            vcpu = 8
            memoryGB = 32
            configs.append(build_config(vcpu, memoryGB, query))

            # E. Avanzar al día siguiente del lote actual
            current_date = batch_end_date + pd.Timedelta(days=1)
        
        return configs


    @task()
    def aggregate_gravity_features_config(**context):
        year = context["params"]["year"]
        query = f"""--sql
    CREATE OR REPLACE TABLE gold_gravity_features AS
    
    WITH aggregated_totals AS (
        -- Sumamos los trozos (Reduce Phase)
        SELECT 
            origin_id,
            destination_id,
            SUM(partial_trips) as actual_trips
        FROM temp_trips_batch_agg
        GROUP BY 1, 2
    )

    SELECT 
        t.origin_id,
        t.destination_id,
        t.actual_trips,
        
        -- Distancia (Pre-calculada en Silver)
        GREATEST(d.distance_meters, 500.0) as dist_meters,
        
        -- Demografía (Usamos el año de referencia configurado)
        CAST(pop.poblacion AS DOUBLE) as P_i,
        CAST(econ.renta_total_euros AS DOUBLE) as E_j,
        
        -- Modelo de Gravedad Crudo
        (CAST(pop.poblacion AS DOUBLE) * CAST(econ.renta_total_euros AS DOUBLE)) 
            / POWER(GREATEST(d.distance_meters, 500.0), 2) as gravity_raw

    FROM aggregated_totals t
    -- JOIN Distancias
    JOIN silver_distances d 
        ON t.origin_id = d.origin_id 
        AND t.destination_id = d.destination_id
    -- JOIN Población Origen
    JOIN silver_demographics pop 
        ON t.origin_id = pop.distrito_id 
        AND pop.year = {year}
    -- JOIN Economía Destino
    JOIN silver_demographics econ 
        ON t.destination_id = econ.distrito_id 
        AND econ.year = {year};
"""
        return build_config(8, 16, query)


    @task()
    def gravity_features_config(**context):
        year = context["params"]["year"]
        date_start = date(*context["params"]["date_start"])
        date_end = date(*context["params"]["date_end"])
        query = f"""--sql
                    CREATE OR REPLACE TABLE gold_gravity_features AS

                    WITH trips AS (
                        -- Agregación masiva inicial
                        SELECT 
                            origin_id, 
                            destination_id, 
                            SUM(travels) as actual_trips
                        FROM silver_trips_filtrado
                        GROUP BY 1, 2
                        ORDER BY origin_id, destination_id

                    )

                    SELECT 
                        t.origin_id,
                        t.destination_id,
                        t.actual_trips,
                        
                        -- Distancia segura (min 500m)
                        GREATEST(d.distance_meters, 500.0) as dist_meters,
                        
                        -- Variables del modelo (Casteamos a DOUBLE para evitar overflow en multiplicaciones)
                        CAST(pop.poblacion AS DOUBLE) as P_i,
                        CAST(econ.renta_total_euros AS DOUBLE) as E_j,
                        
                        -- Gravedad Cruda pre-calculada: (P * E) / d^2
                        (CAST(pop.poblacion AS DOUBLE) * CAST(econ.renta_total_euros AS DOUBLE)) 
                            / POWER(GREATEST(d.distance_meters, 500.0), 2) as gravity_raw

                    FROM trips t

                    -- JOINs con las tablas Silver
                    JOIN silver_distances d 
                        ON t.origin_id = d.origin_id 
                        AND t.destination_id = d.destination_id

                    JOIN silver_demographics pop 
                        ON t.origin_id = pop.distrito_id 
                        AND pop.year = {year}

                    JOIN silver_demographics econ 
                        ON t.destination_id = econ.distrito_id 
                        AND econ.year = {year};"""
        
        vcpu = 8
        memoryGB = 32

        # con = duckdb.connect()
        # secreto(con)
        # con.sql(query)

        return build_config(vcpu, memoryGB, query)


    @task()
    def infrastructure_gaps_config(**context):
        year = context["params"]["year"]
        query = f"""--sql 
                    CREATE OR REPLACE TABLE gold_infrastructure_gaps AS

                    WITH ranked_data AS (
                        -- 1. Detectamos outliers sobre los datos ya preparados
                        SELECT 
                            *,
                            -- Ranking basado en rendimiento (Viajes reales vs Gravedad teórica)
                            PERCENT_RANK() OVER (ORDER BY actual_trips / NULLIF(gravity_raw, 0)) as percentile_rank
                        FROM gold_gravity_features
                    ),

                    clean_data AS (
                        -- 2. Nos quedamos con el núcleo representativo (Quitamos el 5% inferior y superior)
                        SELECT * FROM ranked_data
                        WHERE percentile_rank BETWEEN 0.05 AND 0.95
                    ),

                    calibration AS (
                        -- 3. Calculamos la constante K global usando solo datos limpios
                        SELECT 
                            SUM(actual_trips) / SUM(gravity_raw) as k_factor
                        FROM clean_data
                    )

                    -- 4. Generamos el reporte final
                    SELECT 
                        c.origin_id,
                        c.destination_id,
                        
                        -- Métricas Reales
                        c.actual_trips,
                        CAST(c.dist_meters AS INTEGER) as distance_meters,
                        
                        -- Demanda Potencial ( El Modelo )
                        CAST(
                            (SELECT k_factor FROM calibration) * c.gravity_raw 
                        AS INTEGER) as potential_demand,
                        
                        -- GAP Ratio (Indicador de Negocio)
                        -- Si es < 1: Hay menos viajes de los que la economía predice (¿Falta transporte?)
                        ROUND(
                            c.actual_trips / NULLIF((SELECT k_factor FROM calibration) * c.gravity_raw, 0)
                        , 4) as gap_ratio

                    FROM clean_data c
                    ORDER BY gap_ratio ASC;"""
        
        con = duckdb.connect()
        secreto(con)
        con.sql(query)

        vcpu = 4
        memoryGB = 16

        return build_config(vcpu, memoryGB, query)




    @task()
    def filter_silver_config(**context):
        date_start = date(*context["params"]["date_start"])
        date_end = date(*context["params"]["date_end"])
        polygon = context["params"]["polygon"]
        delta = date_end - date_start
        configs = []

        con = duckdb.connect()
        secreto(con)
        con.sql(f"""--sql
                CREATE OR REPLACE TABLE silver_trips_filtrado AS
                    SELECT 
                        t.date,
                        t.period, 
                        t.origin_id,
                        t.destination_id,
                        t.travels
                    FROM silver_trips t
                    LIMIT 0; """)

        for i in range(delta.days + 1):
            fecha = date_start + timedelta(days=i)

            query = f"""--sql
                    INSERT INTO silver_trips_filtrado
                    WITH zona_estudio AS (
                        SELECT distrito_id
                        FROM silver_geometries
                        WHERE ST_Within(
                            centroide, 
                            ST_Transform(
                                ST_GeomFromText('{polygon}'),
                                'EPSG:4326', 
                                'EPSG:25830'
                            )
                        )
                    )
                    SELECT 
                        t.date,
                        t.period,
                        t.origin_id,
                        t.destination_id,
                        t.travels
                    FROM silver_trips t
                    INNER JOIN zona_estudio origen ON t.origin_id = origen.distrito_id
                    INNER JOIN zona_estudio destino ON t.destination_id = destino.distrito_id
                    WHERE 
                        t.date = '{fecha}';"""
            
            configs.append(build_config(8, 32, query))

        return configs

    @task()
    def create_intermediate_features_table():
        con = duckdb.connect()
        secreto(con)
        con.sql("""--sql
            CREATE OR REPLACE TABLE intermediate_daily_features AS
            SELECT * FROM (
                WITH structure_template AS (
                    SELECT 
                        CAST('2023-01-01' AS DATE) as date, 
                        0 as hour, 
                        0.0 as share
                )
                PIVOT structure_template 
                ON hour IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23)
                USING SUM(share)
            ) WHERE 1=0;""")


    @task()
    def daily_features_config(**context):
        configs = []
        date_start = date(*context["params"]["date_start"])
        date_end = date(*context["params"]["date_end"])
        
        delta = date_end - date_start 

        for i in range(delta.days + 1):
            day = date_start + timedelta(days=i)
            query = f"""--sql
                INSERT INTO intermediate_daily_features
                
                WITH daily_data AS (
                    SELECT 
                        date,
                        CAST(period AS INTEGER) as hour,
                        
                        -- Calculamos suma horaria y suma total del día de una vez
                        SUM(travels) as hour_trips,
                        SUM(SUM(travels)) OVER () as day_total
                        
                    FROM silver_trips_filtrado
                    WHERE date = '{day}' 
                    GROUP BY 1, 2
                )

                -- Pivotamos solo las 24 filas de este día
                PIVOT (
                    SELECT 
                        date, 
                        hour, 
                        hour_trips / NULLIF(day_total, 0) as share 
                    FROM daily_data
                )
                ON hour IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23)
                USING COALESCE(SUM(share), 0.0);
            """

            vcpu = 8
            memoryGB = 32
            configs.append(build_config(vcpu, memoryGB, query))
        return configs


    @task()
    def run_clustering(**context):
        date_start = date(*context["params"]["date_start"])
        date_end = date(*context["params"]["date_end"])

        con = duckdb.connect()
        secreto(con)

        # 1. Cargar datos: Matriz de 365 días x 24 horas
        
        df = con.sql(f"SELECT * FROM intermediate_daily_features WHERE date BETWEEN '{date_start}' AND '{date_end}' ORDER BY date").df()
        
        # Definimos las columnas de características (las horas)
    
        feature_cols = [str(i) for i in range(24)] 
        
        # Extraemos la matriz X y rellenamos nulos por seguridad
        X = df[feature_cols].fillna(0).values

        # 2. Estandarización
        
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # 3. Bucle para encontrar el K óptimo (3, 4, 5) usando Silhouette
        best_k = 0
        best_score = -1
        best_model = None
        
        results = {}

        for k in [3, 4]:
            # n_init=10 asegura que ejecute el algoritmo 10 veces y se quede con la mejor semilla
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = kmeans.fit_predict(X_scaled)
            
            # Calculamos Silhouette Score (-1 a 1, cuanto más alto mejor separación)
            score = silhouette_score(X_scaled, labels)
            results[k] = score
            print(f"Testing K={k}: Silhouette Score = {score:.4f}")
            
            # Guardamos el ganador
            if score > best_score:
                best_score = score
                best_k = k
                best_model = kmeans

        print(f"\nGanador: K={best_k} con Score={best_score:.4f}")

        # 4. Asignamos los clusters finales (0, 1, ... k-1)
        # Usamos las etiquetas del mejor modelo encontrado
        df['pattern_cluster'] = best_model.labels_

        # 5. Guardamos en DuckDB
        # Creamos la tabla gold_date_patterns solo con fecha y el ID numérico del cluster
        con.register('df_clusters', df[['date', 'pattern_cluster']])
        
        con.sql("""
            CREATE OR REPLACE TABLE gold_date_patterns AS 
            SELECT 
                CAST(date AS DATE) as date,
                CAST(pattern_cluster AS INTEGER) as pattern_type -- Guardamos como 0, 1, 2...
            FROM df_clusters
        """)
        
        print(f"Tabla 'gold_date_patterns' creada con {len(df)} filas.")
        print("Distribución de clusters:")
        print(df['pattern_cluster'].value_counts())





    ###################################################################################
    # -- Flujo DAG --
    ###################################################################################

    # Gravity model -------------------------------------------------------------------

    # gold_trips_config = gold_trips_reducida_config()

    # gold_trips = BatchOperator.partial(
    #     task_id='gold_trips',
    #     job_name='gold-trips-job',
    #     job_queue='DuckJobQueue',
    #     job_definition='DuckJobDefinition',
    #     region_name='eu-central-1',
    # ).expand(container_overrides=[gold_trips_config]) 

    # temp_gravity_config = temporary_gravity_features_config()

    # temporary_features = BatchOperator.partial(
    #     task_id='temp_gravity_features',
    #     job_name='temp-gravity-features-job',
    #     job_queue='DuckJobQueue',
    #     job_definition='DuckJobDefinition',
    #     region_name='eu-central-1',
    # ).expand(container_overrides=temp_gravity_config)  

    # aggregate_gravity_config = aggregate_gravity_features_config()

    # aggregate_gravity_features = BatchOperator.partial(
    #     task_id='aggregate_gravity_features',
    #     job_name='aggregate-gravity-features-job',
    #     job_queue='DuckJobQueue',
    #     job_definition='DuckJobDefinition',
    #     region_name='eu-central-1',
    # ).expand(container_overrides=[aggregate_gravity_config]) 

    gold_features_gravity_config = gravity_features_config()

    gold_features_gravity = BatchOperator.partial(
        task_id='transform_features',
        job_name='transform-features-job',
        job_queue='DuckJobQueue',
        job_definition='DuckJobDefinition',
        region_name='eu-central-1',
    ).expand(container_overrides=[gold_features_gravity_config])    

    gold_gaps_config = infrastructure_gaps_config()

    gold_gaps_batch = BatchOperator.partial(
        task_id='transform_gaps',
        job_name='transform-gaps-job',
        job_queue='DuckJobQueue',
        job_definition='DuckJobDefinition',
        region_name='eu-central-1',
    ).expand(container_overrides=[gold_gaps_config])

    #gold_f = gold_features_config()

    #Daily patterns ------------------------------------------------------------------------

    filtrar_silver_configs = filter_silver_config()

    filtrar_silver = BatchOperator.partial(
        task_id='filter_silver',
        job_name='filter-silver-job',
        job_queue='DuckJobQueue',
        job_definition='DuckJobDefinition',
        region_name='eu-central-1',
    ).expand(container_overrides=filtrar_silver_configs)    

    features_table = create_intermediate_features_table()

    daily_config = daily_features_config()

    gold_features = BatchOperator.partial(
        task_id='transform_daily',
        job_name='transform-daily-job',
        job_queue='DuckJobQueue',
        job_definition='DuckJobDefinition',
        region_name='eu-central-1',
    ).expand(container_overrides=daily_config)

    cluster_daily_features = run_clustering()

    # Chaining ------------------------------------------------------------------------

    # temp_gravity_config >> temporary_features >> aggregate_gravity_config >> aggregate_gravity_features

    # aggregate_gravity_features >>gold_gaps_config >> gold_gaps

    filtrar_silver_configs >> filtrar_silver >> features_table >> daily_config >> gold_features >> cluster_daily_features

    filtrar_silver >> gold_features_gravity_config >> gold_features_gravity >> gold_gaps_config >> gold_gaps_batch

    # cluster_daily_features

    #gold_trips_config >> gold_trips

    # gold_features_config >> gold_gaps_config

gold_dag()