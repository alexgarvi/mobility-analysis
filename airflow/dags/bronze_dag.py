from airflow import Dataset
from airflow.sdk import dag, task
from pendulum import datetime
import requests
import re
import urllib.request
import duckdb
import os
from airflow.providers.amazon.aws.operators.batch import BatchOperator
from airflow.hooks.base import BaseHook 
import xml.etree.ElementTree as ET

pg = BaseHook.get_connection("neon_postgres")
aws = BaseHook.get_connection("aws_default")


def get_rss():
    print("Downloading RSS...")
    req = urllib.request.Request("https://movilidad-opendata.mitma.es/RSS.xml", headers={"User-Agent": "MITMA-RSS-parser"})
    txt = urllib.request.urlopen(req).read().decode("utf-8", "ignore")
    os.makedirs("data", exist_ok=True)
    with open("data/RSS.xml", "w") as f:
        f.write(txt)
    return ET.XML(txt)


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
            {"name": "RUTA_S3_DUCKLAKE", "value": "s3://carlos-s3-bdet-ducklake"}]
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

    con.sql("""
        ATTACH 'ducklake:secreto_ducklake' AS mobility_ducklake (DATA_PATH 's3://carlos-s3-bdet-ducklake', OVERRIDE_DATA_PATH TRUE) """)
    con.sql("""
        USE mobility_ducklake """)
    
    print("fin")


@dag(
    dag_id="dag_bronze",
    start_date=datetime(2026, 1, 5),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["ingestion"],
    params = {"search_string": r"2023010[1-2]",
              "rss_url": "https://movilidad-opendata.mitma.es/RSS.xml"}
)


#########################
# Beginning of DAG
#########################
def bronze_dag():

    @task()
    def fetch_trips_urls(datestring, root=None):
        table_title = "Viajes_distritos"
        urls = []
        if root is None:
            root = get_rss()
        for item in root.findall("./channel/item"):
            title = item.find('title').text
            url = item.find("link").text
            if "estudios_basicos" in url and re.search(f"{table_title}", title) and re.search(datestring, title):
                urls.append(url)
        return urls
    
    @task()
    def trips_ingest_config(urls):
        configs = []

        for url in urls:
            source_query = f"SELECT * FROM read_csv('{url}', all_varchar=True, ignore_errors=True)"
            query = f"""
                        MERGE INTO bronze_mitma_viajes_distritos AS target
                        USING ({source_query}) AS source
                        ON target.date = source.date
                            AND target.period = source.period 
                            AND target.origin = source.origin 
                            AND target.destination = source.destination  
                            AND target.distance = source.distance  
                            AND target.activity_origin = source.activity_origin  
                            AND target.activity_destination = source.activity_destination  
                            AND target.study_origin_posible = source.study_origin_posible 
                            AND target.study_destination_posible = source.study_destination_posible 
                            AND target.residence = source.residence 
                            AND target.rent = source.rent 
                            AND target.age = source.age 
                            AND target.gender = source.gender 

                        WHEN MATCHED THEN
                            UPDATE SET
                                travels = source.travels,
                                travels_km = source.travels_km

                        WHEN NOT MATCHED THEN 
                            INSERT (
                            date,
                            period,
                            origin,
                            destination,
                            distance,
                            activity_origin,
                            activity_destination,
                            study_origin_posible,
                            study_destination_posible,
                            residence,
                            rent,
                            age,
                            gender,
                            travels,
                            travels_km
                            )

                            VALUES (
                            source.date,
                            source.period,
                            source.origin,
                            source.destination,
                            source.distance,
                            source.activity_origin,
                            source.activity_destination,
                            source.study_origin_posible,
                            source.study_destination_posible,
                            source.residence,
                            source.rent,
                            source.age,
                            source.gender,
                            source.travels,
                            source.travels_km);

                    """

            vcpu=4
            memoryGB = 16

            configs.append(build_config(vcpu, memoryGB, query))

        return configs
    
    @task()
    def create_bronze_tables():
        trips_query = """
                CREATE TABLE IF NOT EXISTS bronze_mitma_viajes_distritos (
                    date VARCHAR,
                    period VARCHAR,
                    origin VARCHAR,
                    destination VARCHAR,
                    distance VARCHAR,
                    activity_origin VARCHAR,
                    activity_destination VARCHAR,
                    study_origin_posible VARCHAR,
                    study_destination_posible VARCHAR,
                    residence VARCHAR,
                    rent VARCHAR,
                    age VARCHAR,
                    gender VARCHAR,
                    travels VARCHAR,
                    travels_km VARCHAR)"""
        con = duckdb.connect()
        secreto(con)
        # Creando trips
        con.sql(trips_query)
        con.sql("ALTER TABLE bronze_mitma_viajes_distritos SET PARTITIONED BY (MONTH(date))")


    
    # -- Flujo DAG --

    create_tables = create_bronze_tables()

    # viajes

    trips_urls = fetch_trips_urls(datestring=r"2023010[1-2]")

    trip_configs = trips_ingest_config(trips_urls)

    trip_ingest = BatchOperator.partial(
        task_id='ingesta_trips',
        job_name='ingesta-trips-job',
        job_queue='DuckJobQueue',
        job_definition='DuckJobDefinition',
        region_name='eu-central-1',
    ).expand(container_overrides=trip_configs)

    create_tables >> trips_urls >> trip_configs >> trip_ingest




bronze_dag()