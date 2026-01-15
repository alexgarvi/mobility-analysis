# from airflow import Dataset
# from airflow.decorators import dag, task
# from pendulum import datetime
# import requests
# import re
# import urllib.request
# import duckdb
# import os

# #AIRFLOWPATH = os.getenv()
# DB_PATH='include/viajes_mitma2.db'
# # Define the basic parameters of the DAG, like schedule and start_date
# @dag(
#     dag_id="mitma_rss_scrapper",
#     start_date=datetime(2026, 1, 5),
#     schedule=None,
#     catchup=False,
#     doc_md=__doc__,
#     default_args={"owner": "Astro", "retries": 3},
#     tags=["example"],
#     params = {"search_string": "2023010"}
# )


# def mitma_viajes():
#     # Define tasks
#     @task() 
#     def fetch_mitma_urls(**context):
#         """
#         Fetches the RSS feed and filters for URLs containing the DAG parameter.
#         """
#         # Get the parameter from the DAG run configuration
#         search_string = context["params"]["search_string"]

#         rss_url = "https://movilidad-opendata.mitma.es/RSS.xml"
#         # Regex to capture the full URL and the date component
#         pattern = r'(https?://[^\s"<>]*/estudios_basicos/por-distritos/viajes/ficheros-diarios/\d{4}-\d{2}/(\d{8})_Viajes_distritos\.csv\.gz)'

#         print(f"Fetching RSS from {rss_url}...")

#         # Open request with User-Agent to avoid 403 Forbidden
#         req = urllib.request.Request(rss_url, headers={"User-Agent": "MITMA-RSS-parser"})
#         txt = urllib.request.urlopen(req).read().decode("utf-8", "ignore")

#         urls = []
#         # Find all matches
#         matches = re.findall(pattern, txt, re.I)

#         # Sort by date (descending) and filter
#         # matches is a list of tuples: (full_url, date_str)
#         sorted_matches = sorted(set(matches), key=lambda x: x[1], reverse=True)

#         for url, ymd in sorted_matches:
#             if search_string in url:
#                 urls.append(url)

#         print(f"Found {len(urls)} URLs matching '{search_string}'.")
#         return urls

#     @task(max_active_tis_per_dag=1)
#     def ingest_to_duckdb(url):
#         """
#         Ingests a specific URL into DuckDB using MERGE INTO for idempotency.
#         """
#         db_path = DB_PATH
#         table_name = "viajes_dis"

#         # Connect to DuckDB (creates file if not exists)
#         con = duckdb.connect(db_path)


#         try:
#             # 1. Install/Load httpfs to read remote files directly
#             con.sql("INSTALL httpfs; LOAD httpfs;")

#             # 2. Define the remote source query with the extra column
#             # We use duckdb's ability to query the URL directly
#             source_query = f"SELECT *, '{url}' as source_url FROM '{url}'"

#             # 3. Create table if it doesn't exist (using the structure of the first file)
#             # We create it empty first to ensure structure exists for the MERGE
#             con.sql(f"""
#                     CREATE TABLE IF NOT EXISTS {table_name} AS
#                     {source_query} LIMIT 0
#                 """)

#             # 4. Perform the MERGE INTO pattern
#             # Note: For MERGE to work efficiently, we need a unique key.
#             # We use the columns provided to define uniqueness combined with source_url.
#             # If the row exists, we update it; if not, we insert it.

#             print(f"Merging data from {url}...")

#             con.sql(f"""
#                     MERGE INTO {table_name} AS target
#                     USING ({source_query}) AS source
#                     ON target.source_url = source.source_url
#                     AND target.fecha = source.fecha
#                     AND target.periodo = source.periodo
#                     AND target.origen = source.origen
#                     AND target.destino = source.destino
#                     AND target.actividad_origen = source.actividad_origen
#                     AND target.actividad_destino = source.actividad_destino
#                     WHEN MATCHED THEN
#                         UPDATE SET
#                             viajes = source.viajes,
#                             viajes_km = source.viajes_km
#                     WHEN NOT MATCHED THEN
#                         INSERT BY NAME;
#                 """)

#             print(f"Successfully merged {url}")

#         except Exception as e:
#             print(f"Error processing {url}: {e}")
#             raise e
#         finally:
#             con.close()

#     @task
#     def transform_silver_viajes():
#         """
#         Transforms raw data from viajes_dis into silver_viajes.
#         Calculates timestamp 'hora' and removes 'fecha' and 'periodo'.
#         """
#         db_path = DB_PATH
#         source_table = "viajes_dis"
#         target_table = "silver_viajes"

#         con = duckdb.connect(db_path)
#         try:
#             print(f"Transforming data from {source_table} to {target_table}...")

#             # Note: We use lpad on periodo to ensure 0-9 becomes 00-09 for proper parsing if needed,
#             # though DuckDB is often flexible. We also use EXCLUDE to drop the old columns.
#             query = f"""
#                     CREATE OR REPLACE TABLE {target_table} AS
#                     SELECT
#                         strptime(fecha::VARCHAR || LPAD(periodo::VARCHAR, 2, '0'), '%Y%m%d%H') as hora,
#                         * EXCLUDE (fecha, periodo)
#                     FROM {source_table}
#                 """

#             con.sql(query)

#             # Verify count
#             count = con.sql(f"SELECT count(*) FROM {target_table}").fetchone()[0]
#             print(f"Transformation complete. {target_table} has {count} rows.")

#         except Exception as e:
#             print(f"Error during transformation: {e}")
#             raise e
#         finally:
#             con.close()

#     # Use dynamic task mapping to run the print_astronaut_craft task for each
#     # Astronaut in space
#     ingest_to_duckdb.expand(url=fetch_mitma_urls())


# # Instantiate the DAG
# mitma_viajes()