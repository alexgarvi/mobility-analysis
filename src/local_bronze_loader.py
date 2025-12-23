from pathlib import Path
import logging
import duckdb
from variables import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
)

class LocalBronzeLoader:
    def __init__(self, bucket_name: str, ducklake_path: str):
        self.bucket_name = bucket_name
        self.ducklake_path = ducklake_path

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

    def ingest_local_file(
        self,
        table_name: str,
        local_path: str,
        subfolder: str,
        file_format: str = "csv"
    ):
        local_path = Path(local_path)

        s3_path = (
            f"s3://{self.bucket_name}/bronze/"
            f"{subfolder}/{table_name}.parquet"
        )

        logging.info(f"Ingestando {local_path} → {s3_path}")

        con = self._setup_duckdb()

        try:
            if file_format == "csv":
                read_stmt = f"""
                    SELECT *,
                           '{local_path.name}' AS source_url
                    FROM read_csv_auto('{local_path}', all_varchar=True, ignore_errors=True)
                """
            elif file_format == "shp":
                read_stmt = f"""
                    SELECT *,
                           '{local_path.name}' AS source_url
                    FROM st_read('{local_path}')
                """
            else:
                raise ValueError("Formato no soportado")

            con.sql(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS
                {read_stmt}
                LIMIT 0;
            """)

            con.sql(f"""
                COPY (
                    {read_stmt}
                )
                TO '{s3_path}'
                (FORMAT PARQUET);
            """)

            con.sql(f"""
                INSERT INTO {table_name}
                SELECT * FROM read_parquet('{s3_path}');
            """)

            logging.info(f"✔ Tabla {table_name} actualizada")

        finally:
            con.close()
