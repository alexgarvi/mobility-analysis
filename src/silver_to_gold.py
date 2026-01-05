from pathlib import Path
import duckdb

def main():
    con = duckdb.connect()
    con.sql("INSTALL ducklake; LOAD ducklake;")
    con.sql("INSTALL spatial; LOAD spatial;")
    con.sql("INSTALL httpfs; LOAD httpfs;")

    con.sql(f"""
    ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake;

    USE my_ducklake;
        """)

    apply_sql_folder(con, "datalake/gold")

    con.close()

def apply_sql_folder(con, folder):
    sql_files = sorted(Path(folder).glob("*.sql"))
    for sql_path in sql_files:
        print(f"Executing {sql_path.name}")
        con.execute(sql_path.read_text())
        print(f"Executed {sql_path.name}")


if __name__ == "__main__":
    main()
