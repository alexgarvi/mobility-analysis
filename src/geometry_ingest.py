import urllib.request
import re
import xml.etree.ElementTree as ET
import duckdb
from collections import defaultdict
import os
import sys
import argparse

# Add src to path if running from root
sys.path.append(os.path.join(os.getcwd(), 'src'))

import ingestion_map
import mitma_titles

INGESTION_MAP = ingestion_map.ingestion_map
MITMA_TITLES_RECORDS = mitma_titles.mitma_titles_records
MITMA_TITLES_ZONIFICACION = mitma_titles.mitma_titles_zonificacion
INE_TITLES = mitma_titles.ine_titles
CENTROID_TITLES = mitma_titles.centroids_titles
RSS = "https://movilidad-opendata.mitma.es/RSS.xml"
DATE_PATTERN = r"2024070[8-9]" 

def get_rss():
    print("Downloading RSS...")
    req = urllib.request.Request(RSS, headers={"User-Agent": "MITMA-RSS-parser"})
    txt = urllib.request.urlopen(req).read().decode("utf-8", "ignore")
    os.makedirs("data", exist_ok=True)
    with open("data/RSS.xml", "w") as f:
        f.write(txt)
    return ET.XML(txt)

def get_mitma_urls(datestring, tablestring, root=None):
    urls = []
    if root is None:
        root = get_rss()
    for item in root.findall("./channel/item"):
        title = item.find('title').text
        url = item.find("link").text
        if re.search(f"{MITMA_TITLES_RECORDS[tablestring]}", title):
            urls.append(url)
    return urls

def get_mitma_urls_zonificacion(datestring, tablestring, root=None):
    urls = []
    if root is None:
        root = get_rss()
    for item in root.findall("./channel/item"):
        title = item.find('title').text
        url = item.find("link").text
        if "estudios_basicos" in url and re.search(f"{MITMA_TITLES_ZONIFICACION[tablestring]}", title) and re.search(datestring, title):
            urls.append(url)
    return urls

def ingest_mitma_urls(con, urls, tablestring):
    for url in urls:
        print(f"Ingesting {url} into {tablestring}")
        try:
             con.sql(f"DESCRIBE {tablestring}")
        except:
             pass

        sql_file = f'datalake/bronze/{tablestring}.sql'
        if os.path.exists(sql_file):
            with open(sql_file, 'r') as f:
                con.execute(f.read())
        
        try:
            con.sql(f"""
                INSERT INTO {tablestring} 
                SELECT * FROM read_csv('{url}', ignore_errors=True, all_varchar=True, auto_detect=True)
            """)
            print(f"Successfully ingested {url}")
        except Exception as e:
            print(f"Error ingesting {url}: {e}")

def ingest_json(con, tablestring, url):
    print(f"Ingesting JSON {url} into {tablestring}")
    try:
        con.sql(f"""
            CREATE TABLE IF NOT EXISTS {tablestring} AS
            SELECT * FROM read_json('{url}', auto_detect=True)
            """)
        print(f"Successfully ingested {tablestring}")
    except Exception as e:
        print(f"Error ingesting {tablestring}: {e}")

def ingest_sh(con, tablestring, url):
    print(f"Ingesting Shapefile {url} into {tablestring}")
    try:

        source_query = f"SELECT * FROM st_read('{url}')"

        with open("datalake/bronze" + f"/{tablestring}.sql") as f:
            query = f.read()
            con.sql(query)

        with open("datalake/bronze_merges" + f"/{tablestring}.sql") as f:
            query = f.read()
            query = query.replace("source_query", source_query)
            con.sql(query)

        print(f"Successfully ingested {tablestring}")
    except Exception as e:
        print(f"Error ingesting {tablestring}: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date-pattern", default=r"2024070[8-9]", help="Regex pattern for date matching in titles")
    args = parser.parse_args()
    date_pattern = args.date_pattern

    con = duckdb.connect()
    try:
        con.sql("INSTALL ducklake; LOAD ducklake;")
        con.sql("INSTALL spatial; LOAD spatial;")
        con.sql("INSTALL httpfs; LOAD httpfs;")
        
        con.sql(f"""
        ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake;
        USE my_ducklake;
        """)
        
        #root = get_rss()
        
        urls = defaultdict(list)
        ine_urls = defaultdict(list)
        geom_urls = defaultdict(list)

        # 4. Geometry (Centroids)
        for table in CENTROID_TITLES.keys():
            geom_urls[table] += [CENTROID_TITLES[table]]
                
        # Process Geometry
        for table in geom_urls:
            path = geom_urls[table][0]
            if os.path.exists(path):
                ingest_sh(con, table, path)
            else:
                print(f"Geometry file not found: {path}")

    except Exception as e:
        print(f"An error occurred: {e}")
        raise
    finally:
        con.close()

if __name__ == "__main__":
    main()