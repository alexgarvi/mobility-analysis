import duckdb
import gzip


if __name__ == "__main__":
    con = duckdb.connect()
    con.sql(f"""--sql
        ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake;

        USE my_ducklake;
            """)


    print(con.sql("""--sql
            SELECT * FROM bronze_mitma_viajes_distritos LIMIT 10
            """))
    
