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

pg = BaseHook.get_connection("neon_postgres")
aws = BaseHook.get_connection("aws_default")
bucket = "s3://carlos-s3-bdet-ducklake" 


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


@dag(
    dag_id="dag_silver",
    start_date=datetime(2026, 1, 5),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    max_active_tasks=32,
    tags=["silver-transform"],
    params = {"date_start": (2023, 4, 1),
              "date_end": (2023, 4, 12)}
)


#########################################################################################################

# Beginning of DAG

#########################################################################################################


def silver_dag():

    @task()
    def make_connection():
        con = duckdb.connect()
        secreto(con)
        return con


    @task()
    def relacion_transform_config():
        query = f"""
            CREATE OR REPLACE TABLE silver_relacion_ine AS
                SELECT
                    seccion_ine,
                    distrito_ine,
                    municipio_ine,
                    distrito_mitma,
                    municipio_mitma,
                    gau_mitma
                FROM bronze_relacion_ine                 
                WHERE 
                    seccion_ine IS NOT NULL
                    AND distrito_ine IS NOT NULL
                    AND municipio_ine IS NOT NULL
                    AND distrito_mitma IS NOT NULL
                    AND municipio_mitma IS NOT NULL;"""
        
        vcpu = 4
        memoryGB = 16
        con = duckdb.connect()
        secreto(con)
        con.sql(query)
        return build_config(vcpu, memoryGB, query)


    @task()
    def festivos_transform_config():
        query = """
            CREATE OR REPLACE TABLE silver_festivos AS
                SELECT 
                    fecha as date,
                    CAST(es_festivo AS BOOLEAN) as is_holiday,
                    CAST(es_fin_de_semana AS BOOLEAN) as is_weekend
                FROM bronze_festivos"""
        
        vcpu = 4
        memoryGB = 16
        con = duckdb.connect()
        secreto(con)
        con.sql(query)
        return build_config(vcpu, memoryGB, query)


    @task()
    def geometry_transform_configs():
        query = f"""
                CREATE OR REPLACE TABLE silver_geometries AS
                WITH mapa_distritos AS (
                    SELECT DISTINCT 
                        distrito_mitma, 
                        distrito_ine
                    FROM silver_relacion_ine)
                SELECT 
                    m.distrito_ine AS distrito_id,
                    c.centroide,
                    g.geometria
                FROM bronze_zona_distritos_centroides c

                JOIN bronze_zona_distritos_geometria g 
                    ON c.distrito_id = g.distrito_id

                JOIN mapa_distritos m 
                    ON c.distrito_id = m.distrito_mitma"""
        
        vcpu = 4
        memoryGB = 32
        return build_config(vcpu, memoryGB, query)


    @task()
    def ine_transform_configs():
        query = """
                CREATE OR REPLACE TABLE silver_demographics AS

                WITH mapa_secciones AS (
                    SELECT DISTINCT 
                        seccion_ine, 
                        distrito_ine
                    FROM silver_relacion_ine
                    WHERE seccion_ine IS NOT NULL
                ),

                datos_seccion AS (
                    SELECT 
                        p.codigo as seccion_id,
                        YEAR(CAST(p.fecha AS DATE)) as year,
                        
                        p.valor as poblacion_seccion,
                        r.valor as renta_media_seccion,
                        
                        (p.valor * r.valor) as renta_total_seccion

                    FROM bronze_ine_poblacion p
                    INNER JOIN bronze_ine_renta r 
                        ON p.codigo = r.codigo 
                        AND YEAR(CAST(p.fecha AS DATE)) = YEAR(CAST(r.fecha AS DATE))
                )

                SELECT 
                    m.distrito_ine as distrito_id,
                    d.year,
    
                    ROUND(SUM(d.renta_total_seccion), 2) as renta_total_euros,

                    ROUND(SUM(d.renta_total_seccion) / NULLIF(SUM(d.poblacion_seccion), 0), 2) as renta_media_euros,

                    CAST(SUM(d.poblacion_seccion) AS INTEGER) as poblacion

                FROM datos_seccion d
                JOIN mapa_secciones m 
                    ON d.seccion_id = m.seccion_ine

                GROUP BY 
                    1, 2;"""
        vcpu = 4
        memoryGB = 16
        return build_config(vcpu, memoryGB, query)


    @task()
    def trips_transform_configs(**context):
        date_start = date(*context["params"]["date_start"])
        date_end = date(*context["params"]["date_end"])
        delta = date_end - date_start
        current_date = pd.to_datetime(date_start)
        final_date = pd.to_datetime(date_end)

        configs = []

        for i in range(delta.days + 1):
            fecha = date_start + timedelta(days=i)
    
            query = f"""--sql
                BEGIN TRANSACTION;

                DELETE FROM silver_trips WHERE date = '{fecha}'::DATE;

                INSERT INTO silver_trips

                WITH mapa_limpio AS (
                    SELECT DISTINCT 
                        distrito_mitma, 
                        distrito_ine 
                    FROM silver_relacion_ine
                )

                SELECT 
                    vd.date,
                    CAST(vd.period AS INTEGER) as period,
                    
                    mo.distrito_ine as origin_id,
                    md.distrito_ine as destination_id,
                    
                    SUM(vd.travels) as travels,
                    SUM(vd.travels_km) as travels_km

                FROM bronze_mitma_viajes_distritos vd

                JOIN mapa_limpio mo 
                    ON vd.origin = mo.distrito_mitma

                JOIN mapa_limpio md 
                    ON vd.destination = md.distrito_mitma

                WHERE vd.date = '{fecha}'::DATE
                
                GROUP BY 
                    1, 2, 3, 4
                
                ORDER BY 
                    origin_id ASC,
                    destination_id ASC;

                COMMIT;"""


            vcpu = 8
            memoryGB = 32
            configs.append(build_config(vcpu, memoryGB, query))


        return configs


    @task()
    def create_silver_trips():
        query = """
        CREATE TABLE IF NOT EXISTS silver_trips (
            date DATE,
            period INTEGER,
            origin_id VARCHAR,
            destination_id VARCHAR,
            travels DOUBLE,
            travels_km DOUBLE
        );
        ALTER TABLE silver_trips SET PARTITIONED BY (date);"""

        con = duckdb.connect()
        secreto(con)
        con.sql(query)
        con.close()


    @task()
    def distance_create_config():
        query = """
                CREATE OR REPLACE TABLE silver_distances AS
                WITH calculo_unico AS (
                    SELECT
                        A.distrito_id AS origin_id,
                        B.distrito_id AS destination_id,
                        ST_DISTANCE(A.centroide, B.centroide) as distance_meters
                    FROM silver_geometries A
                    JOIN silver_geometries B 
                        ON A.distrito_id < B.distrito_id
                )

                SELECT origin_id, destination_id, distance_meters 
                FROM calculo_unico

                UNION ALL

                SELECT destination_id as origin_id, origin_id as destination_id, distance_meters 
                FROM calculo_unico;"""
        
        vcpu = 4
        memoryGB = 16
        return build_config(vcpu, memoryGB, query)


    ###################################################################################
    # -- Flujo DAG --
    ###################################################################################
        

    # Misc ----------------------------------------------------------------------------

    silver_relacion_config = relacion_transform_config()

    silver_festivos_config = festivos_transform_config()

    # misc_transform = BatchOperator.partial(
    #     task_id='transform_misc',
    #     job_name='transform-misc-job',
    #     job_queue='DuckJobQueue',
    #     job_definition='DuckJobDefinition',
    #     region_name='eu-central-1',
    # ).expand(container_overrides=[silver_relacion_config, silver_festivos_config])

    silver_distance_config = distance_create_config()

    distance_create = BatchOperator.partial(
        task_id='transform-distance',
        job_name='transform-distance-job',
        job_queue='DuckJobQueue',
        job_definition='DuckJobDefinition',
        region_name='eu-central-1',
    ).expand(container_overrides=[silver_distance_config])

    # Geometry ------------------------------------------------------------------------

    silver_geometrias_config = geometry_transform_configs()

    geometry_transform = BatchOperator.partial(
        task_id='transform_geometry',
        job_name='transform-geometry-job',
        job_queue='DuckJobQueue',
        job_definition='DuckJobDefinition',
        region_name='eu-central-1',
    ).expand(container_overrides=[silver_geometrias_config])

    # Ine -----------------------------------------------------------------------------

    silver_ine_config = ine_transform_configs()

    ine_transform = BatchOperator.partial(
        task_id='transform_ine',
        job_name='transform-ine-job',
        job_queue='DuckJobQueue',
        job_definition='DuckJobDefinition',
        region_name='eu-central-1',
    ).expand(container_overrides=[silver_ine_config])

    # Trips ---------------------------------------------------------------------------

    create_silver = create_silver_trips()

    silver_trips_configs = trips_transform_configs()

    trips_transform = BatchOperator.partial(
        task_id='transform-trips',
        job_name='transform-trips-job',
        job_queue='DuckJobQueue',
        job_definition='DuckJobDefinition',
        region_name='eu-central-1',
    ).expand(container_overrides=silver_trips_configs)

    # Chaining ------------------------------------------------------------------------

    [silver_relacion_config, silver_festivos_config] >> trips_transform

    silver_geometrias_config >> geometry_transform >> distance_create

    silver_ine_config >> ine_transform

    create_silver >> silver_trips_configs >> trips_transform

    silver_distance_config >> distance_create

silver_dag()

