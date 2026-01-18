from airflow import Dataset
from airflow.sdk import dag, task
from pendulum import datetime
import duckdb
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.hooks.base import BaseHook 
import boto3
from botocore.exceptions import ClientError
from datetime import date

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
              "date_start": (2023, 1, 1),
              "date_end": (2023, 12, 31)}
)


#########################################################################################################

# Beginning of DAG

#########################################################################################################


def gold_dag():

    @task()
    def gravity_features_config(**context):
        year = context["params"]["year"]
        query = f"""--sql
                    CREATE OR REPLACE TABLE gold_gravity_features AS

                    WITH trips AS (
                        -- Agregación masiva inicial
                        SELECT 
                            origin_id, 
                            destination_id, 
                            SUM(travels) as actual_trips
                        FROM silver_trips
                        WHERE date BETWEEN '{year}-01-01' AND '{year}-12-31'
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
        
        vcpu = 4
        memoryGB = 16

        return build_config(vcpu, memoryGB, query)


    @task()
    def daily_features_config(**context):
        date_start = date(*context["params"]["date_start"])
        date_end = date(*context["params"]["date_end"])
        query = f"""--sql
                CREATE OR REPLACE TABLE intermediate_daily_features AS

                WITH hourly_counts AS (

                    SELECT 
                        date,
                        CAST(period AS INTEGER) as hour,
                        SUM(travels) as total_trips
                    FROM silver_trips
                    WHERE date BETWEEN '{date_start}' AND '{date_end}'
                    GROUP BY 1, 2
                ),

                daily_totals AS (
                    -- 2. Total del día para normalizar (porcentaje)
                    SELECT date, SUM(total_trips) as day_total
                    FROM hourly_counts
                    GROUP BY 1
                ),

                normalized AS (
                    -- 3. Normalizamos: ¿Qué % del tráfico ocurre en esta hora?
                    -- Esto permite comparar un Lunes de Agosto (pocos viajes) con uno de Noviembre (muchos)
                    SELECT 
                        h.date,
                        h.hour,
                        h.total_trips / NULLIF(d.day_total, 0) as share
                    FROM hourly_counts h
                    JOIN daily_totals d ON h.date = d.date
                )

                -- 4. PIVOT: Una fila por día, 24 columnas (h00...h23)
                -- DuckDB tiene una sintaxis PIVOT maravillosa
                PIVOT normalized 
                ON hour IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23)
                USING SUM(share);"""
        
        vcpu = 8
        memoryGB = 32
        return build_config(vcpu, memoryGB, query)


    @task()
    def run_clustering(**context):
        date_start = date(*context["params"]["date_start"])
        date_end = date(*context["params"]["date_end"])

        con = duckdb.connect()
        secreto(con)

        # 1. Cargar datos: Matriz de 365 días x 24 horas
        # Las columnas se llaman "0", "1", ... "23" (strings) debido al PIVOT
        df = con.sql(f"SELECT * FROM intermediate_daily_features WHERE date BETWEEN '{date_start}' AND '{date_end}' ORDER BY date").df()
        
        # Definimos las columnas de características (las horas)
        # Ajusta si tus columnas son ints, pero suelen bajar como strings '0', '1'...
        feature_cols = [str(i) for i in range(24)] 
        
        # Extraemos la matriz X y rellenamos nulos por seguridad
        X = df[feature_cols].fillna(0).values

        # 2. Estandarización
        # Vital para K-Means: pone todas las horas en la misma escala de varianza
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # 3. Bucle para encontrar el K óptimo (3, 4, 5) usando Silhouette
        best_k = 0
        best_score = -1
        best_model = None
        
        results = {}

        for k in [3, 4, 5]:
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

    # gold_features_config = gravity_features_config()

    # gold_features = BatchOperator.partial(
    #     task_id='transform_features',
    #     job_name='transform-features-job',
    #     job_queue='DuckJobQueue',
    #     job_definition='DuckJobDefinition',
    #     region_name='eu-central-1',
    # ).expand(container_overrides=[gold_features_config])    

    # gold_gaps_config = infrastructure_gaps_config()

    # gold_gaps = BatchOperator.partial(
    #     task_id='transform_gaps',
    #     job_name='transform-gaps-job',
    #     job_queue='DuckJobQueue',
    #     job_definition='DuckJobDefinition',
    #     region_name='eu-central-1',
    # ).expand(container_overrides=[gold_gaps_config])

    # Patterns ------------------------------------------------------------------------

    # daily_config = daily_features_config()

    # gold_features = BatchOperator.partial(
    #     task_id='transform_daily',
    #     job_name='transform-daily-job',
    #     job_queue='DuckJobQueue',
    #     job_definition='DuckJobDefinition',
    #     region_name='eu-central-1',
    # ).expand(container_overrides=[daily_config])

    cluster_daily_features = run_clustering()

    # Chaining ------------------------------------------------------------------------

    # gold_gaps_config >> gold_gaps

    # gold_features_config >> gold_features

    # gold_features >> gold_gaps

    # daily_config >> gold_features

    cluster_daily_features

#gold_dag()