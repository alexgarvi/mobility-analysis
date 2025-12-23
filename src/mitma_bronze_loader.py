from datetime import datetime
from pathlib import Path
from typing import List
import logging
from urllib.parse import urlparse
import duckdb
import requests
from variables import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY

class MITMA2023BronzeLoader:
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.base_urls = {
            "distritos": "https://movilidad-opendata.mitma.es/estudios_basicos/por-distritos/viajes/ficheros-diarios/",
            "GAU": "https://movilidad-opendata.mitma.es/estudios_basicos/por-GAU/viajes/ficheros-diarios/",
            "municipios": "https://movilidad-opendata.mitma.es/estudios_basicos/por-municipios/viajes/ficheros-diarios/",
        }

    def get_daily_urls_for_year(self, year: int = 2023) -> List[dict]:
        urls = []
        for data_type, base_url in self.base_urls.items():
            for month in range(1, 13):
                for day in range(1, 32):
                    try:
                        date = datetime(year, month, day)
                        date_str = date.strftime("%Y%m%d")
                        url = f"{base_url}{year}-{month:02d}/{date_str}_Viajes_{data_type}.csv.gz"
                        urls.append({'date': date, 'url': url, 'type': data_type})
                    except ValueError:
                        continue
        return urls

    def parquet_filename_from_url(self, url: str) -> str:
        path = urlparse(url).path
        filename = Path(path).name
        parts = filename.split("_", 1)
        name_part = parts[1].replace(".csv.gz", "").replace(".csv", "") if len(parts) == 2 else filename.replace(".csv.gz", "").replace(".csv", "")
        return f"{name_part.lower()}.parquet"

    def file_exists_s3(self, s3_path: str) -> bool:
        try:
            con = self._setup_duckdb()
            con.sql(f"SELECT * FROM read_parquet('{s3_path}') LIMIT 1").fetchall()
            con.close()
            return True
        except Exception:
            return False

    def _setup_duckdb(self):
        con = duckdb.connect()
        con.sql("INSTALL httpfs; LOAD httpfs;")
        con.sql("INSTALL ducklake; LOAD ducklake;")
        con.sql("INSTALL spatial; LOAD spatial;")
        con.sql(f"""
            SET s3_region='{AWS_REGION}';
            SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';
            SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';
        """)
        
        con.sql(f"""
            ATTACH 'ducklake:{self.ducklake_path}' AS my_ducklake;
            USE my_ducklake;
        """)

        return con

    def download_and_store_daily(self, url: str, date: datetime):
        logging.debug(f"Procesando {url}")
        resp = requests.head(url)
        if resp.status_code != 200:
            logging.warning(f"Archivo no encontrado (404): {url}")
            return

        parquet_name = self.parquet_filename_from_url(url)
        s3_path = f"s3://{self.bucket_name}/bronze/{date.year}/{date.month:02d}/{date.day:02d}/{parquet_name}"

        if self.file_exists_s3(s3_path):
            logging.info(f"Ya existe {s3_path}, se salta.")
            return

        con = self._setup_duckdb()
        try:
            con.sql(f"""
                COPY (
                    SELECT *, '{url}' AS source_url, CURRENT_TIMESTAMP AS ingestion_ts
                    FROM read_csv_auto('{url}', all_varchar=True)
                )
                TO '{s3_path}'
                (FORMAT PARQUET);
            """)
            logging.info(f"Guardado {url} en {s3_path}")
        except Exception as e:
            logging.error(f"Error guardando {url}: {e}")
        finally:
            con.close()

    def load_year(self, year: int = 2023):
        urls = self.get_daily_urls_for_year(year)
        logging.info(f"Encontradas {len(urls)} URLs para {year}")

        successful = 0
        failed = []

        for u in urls:
            try:
                self.download_and_store_daily(u['url'], u['date'])
                successful += 1
            except Exception:
                failed.append(u['date'])
                logging.error(f"Error en {u['date']}")

        logging.info(f"""
Carga completada para {year}:
- Exitosos: {successful}/{len(urls)}
- Fallidos: {len(failed)}
- Fechas fallidas: {failed}
""")