import duckdb
import os

DATA_PATH = "datalake/silver"

def query_silver_table(filename):
    with open(DATA_PATH + f"/{file}") as f:
        query = f.read()
    
    con.sql(query)
    



if __name__ == "__main__":
    con = duckdb.connect()

    con.sql(f"""
    ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake;

    USE my_ducklake;
        """)

    for file in os.listdir(DATA_PATH):
        print(f"Querying {file}")
        query_silver_table(file)