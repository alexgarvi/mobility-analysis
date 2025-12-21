import urllib.request
import re
import xml.etree.ElementTree as ET
import mitma_titles
import duckdb
from collections import defaultdict
import ingestion_map
import glob
import os

INGESTION_MAP = ingestion_map.ingestion_map

MITMA_TITLES_RECORDS = mitma_titles.mitma_titles_records
MITMA_TITLES_ZONIFICACION = mitma_titles.mitma_titles_zonificacion
INE_TITLES = mitma_titles.ine_titles
CENTROID_TITLES = mitma_titles.centroids_titles
RSS = "https://movilidad-opendata.mitma.es/RSS.xml"
DATE_PATTERN = r"2024070[8-9]"

def get_rss():
    req = urllib.request.Request(RSS, headers={"User-Agent": "MITMA-RSS-parser"})
    txt = urllib.request.urlopen(req).read().decode("utf-8", "ignore")

    print("RSS downloaded")
    # matches = re.findall(pattern, txt, re.I)
    return ET.XML(txt)

def get_mitma_urls(datestring, tablestring, root=None):

    urls = []
    if root is None:
        root = get_rss()

    for item in root.findall("./channel/item"):
        title = item.find('title').text
        url = item.find("link").text
        #print(item.find("title").text)
        #print(item.find("link").text)

        if re.search(f"{MITMA_TITLES_RECORDS[tablestring]}", title):
            #print(title)
            #print(url)
            urls.append(url)
    return urls

def get_mitma_urls_zonificacion(datestring, tablestring, root=None):

    urls = []
    if root is None:
        root = get_rss()

    for item in root.findall("./channel/item"):
        title = item.find('title').text
        url = item.find("link").text
        #print(item.find("title").text)
        #print(item.find("link").text)

        if "estudios_basicos" in url and re.search(f"{MITMA_TITLES_ZONIFICACION[tablestring]}", title) and re.search(datestring, title):
            #print(title)
            #print(url)
            urls.append(url)
    return urls


def ingest_url(tablestring, url):

    source_query = f"SELECT * FROM read_csv('{url}', ignore_errors=True)"

    con.sql(f"""
            CREATE TABLE IF NOT EXISTS {tablestring}_test AS
            {source_query} LIMIT 5
            """)
    
    print(con.sql(f"""select * from {tablestring}_test limit 5"""))

    print(con.sql(f"""--sql
                SELECT *
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{tablestring}_test'
                """))

    con.sql(f"""--sql
            DROP TABLE {tablestring}_test
            """)
    
def ingest_json(tablestring, url):

    source_query = f"SELECT * FROM read_json('{url}')"

    con.sql(f"""
            CREATE TABLE IF NOT EXISTS {tablestring}_test AS
            {source_query} LIMIT 5
            """)
    
    print(con.sql(f"""select * from {tablestring}_test limit 5"""))

    print(con.sql(f"""--sql
                SELECT *
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{tablestring}_test'
                """))

    con.sql(f"""--sql
            DROP TABLE {tablestring}_test
            """)
    
def ingest_sh(tablestring, url):

    source_query = f"SELECT * FROM st_read('{url}')"

    con.sql(f"""
            CREATE TABLE IF NOT EXISTS {tablestring}_test AS
            {source_query} LIMIT 5
            """)
    
    print(con.sql(f"""select * from {tablestring}_test limit 5"""))

    print(con.sql(f"""--sql
                SELECT *
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = '{tablestring}_test'
                """))

    con.sql(f"""--sql
            DROP TABLE {tablestring}_test
            """)


def ingest_mitma_urls(urls, tablestring, local=True):

    for url in urls:
        print(url)
        source_query = f"SELECT *, '{url}' as source_url FROM read_csv('{url}', ignore_errors=True)"


        con.sql(f"""--sql
                CREATE TABLE IF NOT EXISTS {tablestring}_test AS
                {source_query} LIMIT 0
                """)
        
        print(con.sql(f"""--sql
                SELECT *
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_NAME = {tablestring}_test
                """))

        con.sql(f"""--sql
                DROP TABLE {tablestring}_test
                """)
        
        return

        con.sql(f"""--sql
                INSERT INTO {tablestring}
                SELECT * FROM ({source_query})
                EXCEPT
                SELECT * FROM {tablestring}
                --WHERE source_url = '{url}';
                """)
            
    print(con.sql(f"""--sql
                    SELECT * FROM {tablestring} limit 10
                    """))


def create_tables(table):
    with open(f'./datalake/bronze/{table}.sql') as f:
        query = f.read()

    con.sql(query)



def merge_into(table, url, local=True):
    print(url)
    source_query = f"SELECT * FROM read_csv('{url}', all_varchar=True, ignore_errors=True)"
    with open(f'./datalake/bronze_merges/{table}.sql') as f:
        merge_query = f.read()
    
    merge_query = merge_query.replace('source_query', source_query)
    
    con.sql(merge_query)


if __name__ == "__main__":

    # Duckdb Boilerplate
    ###########################################################

    con = duckdb.connect()
    con.sql("INSTALL ducklake; LOAD ducklake;")
    con.sql("INSTALL spatial; LOAD spatial;")
    con.sql("INSTALL httpfs; LOAD httpfs;")

    con.sql(f"""
    ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake;

    USE my_ducklake;
        """)
    
    ###########################################################

    # Fetching URLs
    ###########################################################

    with open("data/RSS.xml") as f:
        txt = f.read()
        
    root = ET.XML(txt)
    urls = defaultdict(list)
    ine_urls = defaultdict(list)
    geom_urls = defaultdict(list)
        
    for table in MITMA_TITLES_RECORDS.keys():
        if table == 'bronze_provincias' or table == 'bronze_festivos':
            urls[table] += [MITMA_TITLES_RECORDS[table]]
        else:
            urls[table] += get_mitma_urls(DATE_PATTERN, table, root=root)

    for table in MITMA_TITLES_ZONIFICACION.keys():
        urls[table] += get_mitma_urls_zonificacion(DATE_PATTERN, table, root=root)

    for table in INE_TITLES.keys():
        ine_urls[table] += [INE_TITLES[table]]

    for table in CENTROID_TITLES.keys():
        geom_urls[table] += [CENTROID_TITLES[table]]


    ###########################################################

    # Ingesting URLs
    ###########################################################

    for t in urls:
        print(f'doing {t}')
        #ingest_url(t, urls[t][0])

    for t in ine_urls:
        print(f"doing {t}")
        ingest_json(t, ine_urls[t][0])

    for t in geom_urls:
        print(f"doing {t}")
        ingest_sh(t, geom_urls[t][0])

    ###########################################################




if 1==2:
    for table in ['bronze_mitma_viajes_distritos']:

        create_tables(table)
        #con.sql(f'DROP TABLE {table}')
        path = INGESTION_MAP[table]
        found_files = glob.glob(os.path.join(path, '*20230106*.csv.gz'))
        found_files += glob.glob(os.path.join(path, '*20230107*.csv.gz'))

        for url in [found_files[0]]:
            merge_into(table, url)

        print(con.sql(f"SELECT * FROM  {table} LIMIT 10"))








if 0 == 1:

    con = duckdb.connect()
    con.sql("INSTALL ducklake; LOAD ducklake;")
    con.sql("INSTALL spatial; LOAD spatial;")
    con.sql("INSTALL httpfs; LOAD httpfs;")

    con.sql(f"""
    ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake;

    USE my_ducklake;
        """)

    #INGESTION_MAP = {'bronze_festivos': './data/festivos.csv'}

    local = True
    if local:
        for table in INGESTION_MAP:
            path = INGESTION_MAP[table]
            found_files = glob.glob(os.path.join(path, '*20230106*.csv.gz'))
            found_files += glob.glob(os.path.join(path, '*20230107*.csv.gz'))

            if len(found_files) == 0:
                found_files = [path]

            print(f"Ingesting {table}")
            ingest_mitma_urls(found_files, table)



    else:

        
        with open("data/RSS.xml") as f:
            txt = f.read()
        
        root = ET.XML(txt)

        urls = defaultdict(list)

        # INGESTAR MITMA
        
        for table in MITMA_TITLES_RECORDS.keys():
            urls[table] += get_mitma_urls(DATE_PATTERN, table, root=root)
            #print(urls[table])

        for table in urls:
            print(f"Ingesting {table}")
            ingest_mitma_urls(urls[table], table)
        
        # INGESTAR ZONIFICACION
        
        print(f"Ingesting bronze_zona_distritos_centroides")
        ingest_mitma_urls(['./data/mitma/zonificacion/distritos/distritos_centroides.csv'], 'bronze_zona_distritos_centroides')

        print(f"Ingesting bronze_zona_gaus_centroides")
        ingest_mitma_urls(['./data/mitma/zonificacion/gau/gaus_centroides.csv'], 'bronze_zona_gaus_centroides')

        print(f"Ingesting bronze_zona_municipios_centroides")
        ingest_mitma_urls(['./data/mitma/zonificacion/municipios/municipios_centroides.csv'], 'bronze_zona_municipios_centroides')

        #INGESTAR INE



