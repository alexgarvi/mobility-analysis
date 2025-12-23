import logging
import src.ingestion_map
from src.local_bronze_loader import LocalBronzeLoader
from src.mitma_bronze_loader import MITMA2023BronzeLoader
from variables import BUCKET_NAME

def run_mitma_ingestion(year: int = 2023):
    logging.basicConfig(level=logging.INFO)

    loader = MITMA2023BronzeLoader(BUCKET_NAME)
    loader.load_year(year=year)

def run_local_ingestion():
    logging.basicConfig(level=logging.INFO)

    loader = LocalBronzeLoader(
        bucket_name=BUCKET_NAME,
        ducklake_path="my_ducklake.ducklake"
    )

    for table in src.ingestion_map.ingestion_map:
        logging.info(f"Ingestando {table}")

        path = src.ingestion_map.ingestion_map[table]

        if path.endswith(".csv"):
            loader.ingest_local_file(
                table_name=table,
                local_path=path,
                subfolder=table.replace("bronze_", ""),
                file_format="csv"
            )