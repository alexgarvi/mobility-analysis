import duckdb
from src.mitma_bronze_loader import MITMA2023BronzeLoader
from variables import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY, BUCKET_NAME
    
def setup_duckdb():
    con = duckdb.connect()

    con.sql("INSTALL httpfs; LOAD httpfs;")
    con.sql("INSTALL ducklake; LOAD ducklake;")
    con.sql("INSTALL spatial; LOAD spatial;")

    # Credenciales S3
    con.sql(f"""
        SET s3_region='{AWS_REGION}';
        SET s3_access_key_id='{AWS_ACCESS_KEY_ID}';
        SET s3_secret_access_key='{AWS_SECRET_ACCESS_KEY}';
    """)

    return con

if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)

    con = setup_duckdb()
    loader = MITMA2023BronzeLoader(con, BUCKET_NAME)

    loader.load_year(year=2023)

    con.close()