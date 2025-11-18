import os
import glob
import gzip
import duckdb
import pandas as pd
from ingestion_map import ingestion_map

con = duckdb.connect('datalake.duckdb')

for table_name, data_path in ingestion_map.items():
    sql_file_path = os.path.join('.', 'datalake', 'bronze', f'{table_name}.sql')
    
    with open(sql_file_path, 'r') as f:
        con.execute(f.read())

    sample_file = data_path
    is_gzipped = False

    if os.path.isdir(data_path):
        found_files = glob.glob(os.path.join(data_path, '*2023*.csv.gz'))
        if found_files:
            sample_file = found_files[0]
            is_gzipped = True

    delimiter = ';'
    try:
        if is_gzipped:
            with gzip.open(sample_file, 'rt') as f:
                first_line = f.readline()
        else:
            with open(sample_file, 'r') as f:
                first_line = f.readline()
        
        if first_line.count('|') > first_line.count(';'):
            delimiter = '|'
    except Exception:
        pass

    if os.path.isdir(data_path):
        source_pattern = glob.glob(os.path.join(data_path, '*2023*.csv.gz'))
        print(pd.read_csv(source_pattern[0], encoding='UTF-8', delimiter='|').head(2))
        try:
            con.execute(f"INSERT INTO {table_name} SELECT * FROM read_csv({source_pattern}, QUOTE='', ENCODING='UTF-8', AUTO_DETECT=TRUE, HEADER=TRUE, COMPRESSION='GZIP', IGNORE_ERRORS=TRUE, DELIM='{delimiter}')")
            print('succesfully ingested', source_pattern)
        except Exception as e:
            with open('trace.txt', 'a') as file:
                print('error ingesting', data_path, e, file=file)
    else:
        try:
            con.execute(f"INSERT INTO {table_name} SELECT * FROM read_csv('{data_path}', QUOTE='', ENCODING='UTF-8', AUTO_DETECT=TRUE, HEADER=TRUE, IGNORE_ERRORS=TRUE, DELIM='{delimiter}')")
            print('succesfully ingested', data_path)
        except Exception as e:
            with open('trace.txt', 'a') as file:
                print('error ingesting', data_path, e, file=file)

con.close()