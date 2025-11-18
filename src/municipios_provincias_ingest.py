import duckdb

con = duckdb.connect('datalake.duckdb')

con.execute(f'CREATE OR REPLACE TABLE bronze_provincias AS SELECT * FROM read_csv("./data/mitma/zonificacion/provincias.csv")')

municipios_path = './data/mitma/zonificacion/municipios/nombres_municipios.csv'

with open('./datalake/bronze/bronze_municipios_nombres.sql') as sql_file:
    con.execute(sql_file.read())

con.execute(f"INSERT INTO bronze_municipios_nombres SELECT * FROM read_csv('{municipios_path}', QUOTE='', ENCODING='UTF-8', AUTO_DETECT=TRUE, HEADER=TRUE, IGNORE_ERRORS=TRUE, DELIM='|')")


