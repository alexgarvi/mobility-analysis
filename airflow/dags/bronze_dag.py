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
from datetime import date, timedelta
import boto3
from botocore.exceptions import ClientError
import holidays
import io

pg = BaseHook.get_connection("neon_postgres")
aws = BaseHook.get_connection("aws_default")
bucket = "s3://carlos-s3-bdet-ducklake" 


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


def fetch_trips_url(day, root):
    datestring = day.replace("-", "")
    table_title = "Viajes_distritos"
    urls = []
    for item in root.findall("./channel/item"):
        title = item.find('title').text
        url = item.find("link").text
        if "estudios_basicos" in url and re.search(f"{table_title}", title) and re.search(datestring, title):
            return (url, day)


def ine_urls(ids):
    urls = []
    for id in ids:
        urls.append(f"https://www.ine.es/jaxiT3/files/t/es/json/{id}.json")
    return urls


@dag(
    dag_id="dag_bronze",
    start_date=datetime(2026, 1, 5),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    max_active_tasks=32,
    tags=["ingestion"],
    params = {"date_start": (2023, 1, 1),
              "date_end": (2023, 12, 31),
              "rss_url": "https://movilidad-opendata.mitma.es/RSS.xml"}
)


#########################################################################################################

# Beginning of DAG

#########################################################################################################


def bronze_dag():

    @task()
    def get_misc_urls():
        urls = []
        root = get_rss()
        for item in root.findall("./channel/item"):
            title = item.find('title').text
            url = item.find("link").text
            if "nombres_distritos.csv" in title:
                urls.append((url, "bronze_distritos_nombres"))
            if "nombres_gaus.csv" in title:
                urls.append((url, "bronze_gaus_nombres"))
            if "nombres_municipios.csv" in title:
                urls.append((url, "bronze_municipios_nombres"))
            if "relacion_ine_zonificacionMitma.csv" in title:
                urls.append((url, "bronze_relacion_ine"))

        urls.append(('s3://carlos-s3-bdet-ducklake/festivos/festivos.csv', "bronze_festivos"))
        return urls


    @task() 
    def misc_ingest_config(urls):
        configs = []
        for url in urls:
            if url[1] == "bronze_distritos_nombres":
                query = f"""
                            CREATE OR REPLACE TABLE {url[1]} AS
                            SELECT 
                                ID,
                                name
                            FROM read_csv('{url[0]}', 
                                        auto_detect=TRUE, 
                                        header=TRUE,
                                        sep='|');
                        """
            if url[1] == "bronze_gaus_nombres":
                query = f"""
                            CREATE OR REPLACE TABLE {url[1]} AS
                            SELECT 
                                ID,
                                name
                            FROM read_csv('{url[0]}', 
                                        auto_detect=TRUE, 
                                        header=TRUE,
                                        sep='|');
                        """
            if url[1] == "bronze_municipios_nombres":
                query = f"""
                            CREATE OR REPLACE TABLE {url[1]} AS
                            SELECT 
                                ID,
                                name
                            FROM read_csv('{url[0]}', 
                                        auto_detect=TRUE, 
                                        header=TRUE,
                                        sep='|');
                        """
            if url[1] == "bronze_relacion_ine":
                query = f"""
                            CREATE OR REPLACE TABLE {url[1]} AS
                            SELECT 
                                seccion_ine,
                                distrito_ine,
                                municipio_ine,
                                distrito_mitma,
                                municipio_mitma,
                                gau_mitma
                            FROM read_csv('{url[0]}', 
                                        auto_detect=TRUE, 
                                        header=TRUE,
                                        sep='|');
                        """
            if url[1] == "bronze_festivos":
                query = f"""
                            CREATE OR REPLACE TABLE {url[1]} AS
                            SELECT 
                                fecha,
                                es_festivo,
                                es_fin_de_semana
                            FROM read_csv('{url[0]}', 
                                        auto_detect=TRUE, 
                                        header=TRUE);
                        """
            vcpu=4
            memoryGB = 8

            configs.append(build_config(vcpu, memoryGB, query))
        return configs


    @task()
    def get_geometry_urls():
        urls = []
        root = get_rss()
        for item in root.findall("./channel/item"):
            title = item.find('title').text
            url = item.find("link").text
            if "zonificacion_distritos" in title:
                urls.append(url)
        return urls


    @task()
    def download_to_bucket(lista_urls, bucket_name, carpeta_destino, aws_region='eu-central-1'):
        s3_client = boto3.client('s3', region_name=aws_region, 
                                 aws_access_key_id=aws.login,
                                 aws_secret_access_key=aws.password)
        
        print(f"Iniciando comprobación de {len(lista_urls)} archivos...")

        for url in lista_urls:
            nombre_archivo = os.path.basename(url)
            s3_key = f"{carpeta_destino}/{nombre_archivo}"

            # Comprobar si existe
            try:
                # head_object pide solo los metadatos (no descarga el archivo), es muy rápido y barato
                s3_client.head_object(Bucket=bucket_name, Key=s3_key)
                print(f"El archivo {nombre_archivo} YA EXISTE. Saltando...")
                continue # Pasa al siguiente archivo del bucle
            except ClientError:
                # Si da error 404 (Not Found), es que no existe. Seguimos con la descarga.
                pass 

            try:
                print(f"⬇Archivo nuevo detectado. Transfiriendo: {nombre_archivo} ...")
                with requests.get(url, stream=True) as r:
                    if r.status_code == 200:
                        s3_client.upload_fileobj(r.raw, bucket_name, s3_key)
                    else:
                        print(f"Error en la fuente {url}: {r.status_code}")
            except Exception as e:
                print(f"Error subiendo: {e}")

        print("Proceso finalizado.")


    @task()
    def geometries_ingest_config():
        configs = []
        query = f"""
                    CREATE OR REPLACE TABLE bronze_zona_distritos_centroides AS
                    SELECT 
                        ID as distrito_id,                        
                        geom as centroide
                    FROM st_read('s3://carlos-s3-bdet-ducklake/geometries/zonificacion_distritos_centroides.shp');
                """
        query2 = f"""
                    CREATE OR REPLACE TABLE bronze_zona_distritos_geometria AS
                    SELECT 
                        ID as distrito_id,                        
                        geom as geometria
                    FROM st_read('s3://carlos-s3-bdet-ducklake/geometries/zonificacion_distritos.shp');
                """

        vcpu=1
        memoryGB = 8

        configs.append(build_config(vcpu, memoryGB, query))
        configs.append(build_config(vcpu, memoryGB, query2))

        return configs


    @task()
    def get_trips_range(**context):
        date_start = date(*context["params"]["date_start"])
        date_end = date(*context["params"]["date_end"])
        root = get_rss()
        print("root done")
        urls = []
        delta = date_end - date_start   # returns timedelta

        for i in range(delta.days + 1):
            day = date_start + timedelta(days=i)
            url = fetch_trips_url(str(day), root)
            if url:
                urls.append(url)

        return urls


    @task()
    def trips_ingest_config(urls):
        configs = []

        for url in urls:
            query = f"""
                    BEGIN TRANSACTION;
                    DELETE FROM bronze_mitma_viajes_distritos 
                    WHERE date = '{url[1]}'::DATE;

                    INSERT INTO bronze_mitma_viajes_distritos 
                    SELECT 
                        strptime(fecha, '%Y%m%d')::DATE as date,
                        periodo,
                        origen,
                        destino,
                        distancia,
                        actividad_origen,
                        actividad_destino,
                        estudio_origen_posible,
                        estudio_destino_posible,
                        residencia,
                        renta,
                        edad,
                        sexo,
                        SUM(CAST(viajes AS DOUBLE)) as travels,
                        SUM(CAST(viajes_km AS DOUBLE)) as travels_km

                    FROM read_csv('{url[0]}', all_varchar=True, ignore_errors=True)
                    WHERE strptime(fecha, '%Y%m%d')::DATE = '{url[1]}'::DATE
                    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13;

                    COMMIT;
                    """

            vcpu=8
            memoryGB = 32

            configs.append(build_config(vcpu, memoryGB, query))

        return configs


    @task()
    def create_bronze_tables():
        trips_query = """
                CREATE TABLE IF NOT EXISTS bronze_mitma_viajes_distritos (
                    date DATE,
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
                    travels DOUBLE,
                    travels_km DOUBLE)"""
        con = duckdb.connect()
        secreto(con)
        # Creando trips
        con.sql(trips_query)
        con.sql("ALTER TABLE bronze_mitma_viajes_distritos SET PARTITIONED BY (date)")


    @task()
    def ine_ingest_config():
        configs = []

        query_renta = """
                        CREATE OR REPLACE TABLE bronze_ine_renta AS

                        SELECT 

                            MetaData[1].Codigo as codigo,
                            
                            d.Fecha as fecha,
                            
                            TRY_CAST(d.Valor AS DOUBLE) as valor

                        FROM read_json('s3://carlos-s3-bdet-ducklake/ine/30656.json', compression='gzip', auto_detect=true)

                        CROSS JOIN UNNEST(Data) AS t(d)

                        WHERE 
                            MetaData[1].T3_Variable = 'Secciones'
                            
                            AND MetaData[3].Nombre = 'Renta neta media por persona';
                        """

        query_poblacion = """
                            CREATE OR REPLACE TABLE bronze_ine_poblacion AS

                            SELECT 
                                MetaData[1].Codigo as codigo,
                                
                                d.Fecha as fecha,
                                TRY_CAST(d.Valor AS DOUBLE) as valor

                            FROM read_json('s3://carlos-s3-bdet-ducklake/ine/66595.json', compression='gzip', auto_detect=true)
                            CROSS JOIN UNNEST(Data) AS t(d)

                            WHERE 
                                MetaData[1].T3_Variable = 'Secciones'
                                
                                AND MetaData[2].Nombre = '16 y más años'
                                
                                AND MetaData[3].Nombre = 'Total'
                                
                                AND MetaData[4].Nombre = 'Total'
                                
                                AND MetaData[5].Nombre = 'Total';
                            """
        vcpu=4
        memoryGB = 16

        configs.append(build_config(vcpu, memoryGB, query_renta))
        configs.append(build_config(vcpu, memoryGB, query_poblacion))

        return configs


    @task()
    def generar_y_subir_festivos(**context):
        # --- 1. CONFIGURACIÓN ---
        BUCKET_NAME = 'carlos-s3-bdet-ducklake'
        S3_KEY = 'festivos/festivos.csv'  # Ruta dentro del bucket
        
        # Intenta coger las claves de las variables de entorno, o ponlas aquí directamente si es solo para ti
        AWS_ACCESS_KEY = aws.login     # O pon 'TU_CLAVE_AQUI'
        AWS_SECRET_KEY = aws.password # O pon 'TU_SECRET_AQUI'
        REGION = "eu-central-1"

        print("Generando festivos...")

        # --- 2. LÓGICA DE GENERACIÓN (Tu código) ---
        es_holidays = holidays.country_holidays("ES")
        d1 = date(*context["params"]["date_start"])
        d2 = date(*context["params"]["date_end"])
        delta = d2 - d1 

        # Usamos StringIO para crear el CSV en memoria RAM (sin guardar archivo en disco)
        csv_buffer = io.StringIO()
        csv_buffer.write('fecha,es_festivo,es_fin_de_semana\n') # Cabecera

        for i in range(delta.days + 1):
            day = d1 + timedelta(days=i)
            is_festivo = 1 if day in es_holidays else 0
            is_finde = 1 if day.weekday() > 4 else 0 # 5=Sab, 6=Dom
            
            # Escribimos la línea en el buffer
            csv_buffer.write(f'{day},{is_festivo},{is_finde}\n')

        print(f"CSV generado en memoria. Tamaño: {len(csv_buffer.getvalue())} bytes.")

        # --- 3. SUBIDA A S3 ---
        print(f"Subiendo a s3://{BUCKET_NAME}/{S3_KEY} ...")
        
        try:
            s3_client = boto3.client(
                's3', 
                region_name=REGION,
                aws_access_key_id=AWS_ACCESS_KEY,
                aws_secret_access_key=AWS_SECRET_KEY
            )

            s3_client.put_object(
                Body=csv_buffer.getvalue(),
                Bucket=BUCKET_NAME,
                Key=S3_KEY
            )
            print("Archivo subido correctamente.")
            
        except Exception as e:
            print(f"Error al subir a S3: {e}")


    ###################################################################################
    # -- Flujo DAG --
    ###################################################################################

    create_tables = create_bronze_tables()

    # Viajes --------------------------------------------------------------------------

    trips_urls = get_trips_range()

    trip_configs = trips_ingest_config(trips_urls)

    trip_ingest = BatchOperator.partial(
        task_id='ingesta_trips',
        job_name='ingesta-trips-job',
        job_queue='DuckJobQueue',
        job_definition='DuckJobDefinition',
        region_name='eu-central-1',
    ).expand(container_overrides=trip_configs)

    # Geometrias ----------------------------------------------------------------------

    geometry_urls = get_geometry_urls()

    download_geometries = download_to_bucket(geometry_urls, bucket_name=bucket[5:], carpeta_destino="geometries")

    geometry_configs = geometries_ingest_config()

    geometry_ingest = BatchOperator.partial(
        task_id='ingesta_geometry',
        job_name='ingesta-geometry-job',
        job_queue='DuckJobQueue',
        job_definition='DuckJobDefinition',
        region_name='eu-central-1',
    ).expand(container_overrides=geometry_configs)

    # INE -----------------------------------------------------------------------------

    download_ine = download_to_bucket(ine_urls(['30656', '66595']), bucket_name=bucket[5:], carpeta_destino="ine")

    ine_configs = ine_ingest_config()

    ine_ingest = BatchOperator.partial(
        task_id='ingesta_ine',
        job_name='ingesta-ine-job',
        job_queue='DuckJobQueue',
        job_definition='DuckJobDefinition',
        region_name='eu-central-1',
    ).expand(container_overrides=ine_configs)

    # Misc ----------------------------------------------------------------------------

    crear_festivos = generar_y_subir_festivos()

    misc_urls = get_misc_urls()

    misc_configs = misc_ingest_config(misc_urls)

    misc_ingest = BatchOperator.partial(
        task_id='ingesta_misc',
        job_name='ingesta-misc-job',
        job_queue='DuckJobQueue',
        job_definition='DuckJobDefinition',
        region_name='eu-central-1',
    ).expand(container_overrides=misc_configs)

    # Chaining ------------------------------------------------------------------------

    create_tables >> trips_urls >> trip_configs >> trip_ingest

    geometry_urls >> download_geometries >> geometry_configs >> geometry_ingest

    download_ine >> ine_configs >> ine_ingest

    crear_festivos >> misc_urls >> misc_configs >> misc_ingest



#bronze_dag()