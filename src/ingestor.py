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

MITMA_TITLES_0 = mitma_titles.mitma_titles[0]
MITMA_TITLES_1 = mitma_titles.mitma_titles[1]
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

        if "estudios_basicos" in url and re.search(datestring, title) and re.search(f"_{MITMA_TITLES_0[tablestring]}.csv.gz", title):
            #print(title)
            #print(url)
            urls.append(url)
    return urls

def get_mitma_urls_zonificacion(tablestring, root=None):

    urls = []
    if root is None:
        root = get_rss()

    for item in root.findall("./channel/item"):
        title = item.find('title').text
        url = item.find("link").text
        #print(item.find("title").text)
        #print(item.find("link").text)

        if re.search(f"{MITMA_TITLES_1[tablestring]}", title):
            #print(title)
            #print(url)
            urls.append(url)
    return urls

def ingest_mitma_urls(urls, tablestring, local=True):

    if "_ine_" in tablestring:
        encoding = "ANSI"
    else:
        encoding = "UTF-8"

    for url in urls:
        print(url)
        source_query = f"SELECT *, '{url}' as source_url FROM read_csv('{url}', all_varchar=True, ignore_errors=True)"
        con.sql(f"""
                CREATE TABLE IF NOT EXISTS {tablestring} AS
                {source_query} LIMIT 0
                """)
        
        #print(con.sql(f"SELECT * FROM ({source_query})"))
        #print(con.sql(f"SELECT * FROM {tablestring}"))

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




if __name__ == "__main__":


    con = duckdb.connect()
    con.sql("INSTALL ducklake; LOAD ducklake;")
    con.sql("INSTALL spatial; LOAD spatial;")
    con.sql("INSTALL httpfs; LOAD httpfs;")

    con.sql(f"""
    ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake;

    USE my_ducklake;
        """)

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
        
        for table in MITMA_TITLES_0.keys():
            urls[table] += get_mitma_urls(DATE_PATTERN, table, root=root)
            #print(urls[table])

        for table in urls:
            print(f"Ingesting {table}")
            ingest_mitma_urls(urls[table], table)
        
        # INGESTAR ZONIFICACION

        print(f"Ingesting bronze_mitma_relacion_ine_zonificacion")
        url = get_mitma_urls_zonificacion('bronze_mitma_relacion_ine_zonificacion', root = root)
        ingest_mitma_urls(url, 'bronze_mitma_relacion_ine_zonificacion')
        
        print(f"Ingesting bronze_zona_distritos_centroides")
        ingest_mitma_urls(['./data/mitma/zonificacion/distritos/distritos_centroides.csv'], 'bronze_zona_distritos_centroides')

        print(f"Ingesting bronze_zona_gaus_centroides")
        ingest_mitma_urls(['./data/mitma/zonificacion/gau/gaus_centroides.csv'], 'bronze_zona_gaus_centroides')

        print(f"Ingesting bronze_zona_municipios_centroides")
        ingest_mitma_urls(['./data/mitma/zonificacion/municipios/municipios_centroides.csv'], 'bronze_zona_municipios_centroides')

        #INGESTAR INE



