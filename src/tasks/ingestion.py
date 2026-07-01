"""Tâche d'ingestion pour Prefect"""
import time
import os
import json
import csv
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import sqlalchemy as sa
from prefect import task
from ..utils.mongodb import insert_raw_data, save_metrics
from ..utils.metrics import get_cpu_usage, get_memory_mb


def read_csv_file(file_path):
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    return data[0] if data else {}


def read_json_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def read_xml_file(file_path):
    import xml.etree.ElementTree as ET
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = {}
    for child in root:
        data[child.tag] = child.text
    return data


def read_excel_file(file_path):
    df = pd.read_excel(file_path)
    return df.to_dict('records')[0] if not df.empty else {}


def read_parquet_file(file_path):
    df = pd.read_parquet(file_path)
    return df.to_dict('records')[0] if not df.empty else {}


def read_html_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    
    data = {}
    titles = soup.find_all(['h1', 'h2', 'h3'])
    if titles:
        data['titles'] = [t.get_text(strip=True) for t in titles[:5]]
    
    paragraphs = soup.find_all('p')
    if paragraphs:
        data['paragraphs'] = [p.get_text(strip=True) for p in paragraphs[:5]]
    
    tables = soup.find_all('table')
    if tables:
        table_data = []
        for table in tables[:2]:
            rows = table.find_all('tr')
            for row in rows:
                cells = [c.get_text(strip=True) for c in row.find_all(['td', 'th'])]
                if cells:
                    table_data.append(cells)
        data['tables'] = table_data
    
    return data


def read_sql_database(connection_string, query):
    engine = sa.create_engine(connection_string)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)
    return df.to_dict('records')[0] if not df.empty else {}


def fetch_api_rest(url, method='GET', headers=None, params=None, data=None):
    if method.upper() == 'GET':
        response = requests.get(url, headers=headers, params=params)
    elif method.upper() == 'POST':
        response = requests.post(url, headers=headers, json=data)
    elif method.upper() == 'PUT':
        response = requests.put(url, headers=headers, json=data)
    elif method.upper() == 'DELETE':
        response = requests.delete(url, headers=headers)
    else:
        raise ValueError(f"Méthode non supportée: {method}")
    
    response.raise_for_status()
    return response.json()


def fetch_api_graphql(url, query, variables=None, headers=None):
    payload = {'query': query}
    if variables:
        payload['variables'] = variables
    
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def detect_source_type(source_type, source_path):
    if source_type:
        return source_type
    if source_path.startswith(('http://', 'https://')):
        return 'api'
    if source_path.startswith(('postgresql://', 'mysql://', 'sqlite://')):
        return 'sql'
    ext = os.path.splitext(source_path)[1].lower()
    mapping = {
        '.csv': 'csv', '.json': 'json', '.xml': 'xml',
        '.xlsx': 'excel', '.xls': 'excel',
        '.parquet': 'parquet',
        '.html': 'html', '.htm': 'html'
    }
    return mapping.get(ext, 'unknown')


def read_data_from_source(source_type, source_path, file_path=None, **kwargs):
    if source_type in ['csv', 'json', 'xml', 'excel', 'parquet', 'html']:
        actual_path = file_path or source_path
        if not os.path.exists(actual_path):
            raise FileNotFoundError(f"Fichier introuvable: {actual_path}")
        
        readers = {
            'csv': read_csv_file,
            'json': read_json_file,
            'xml': read_xml_file,
            'excel': read_excel_file,
            'parquet': read_parquet_file,
            'html': read_html_file
        }
        return readers[source_type](actual_path)
    
    elif source_type == 'api':
        url = source_path
        method = kwargs.get('method', 'GET')
        headers = kwargs.get('headers', {})
        data = kwargs.get('data', {})
        params = kwargs.get('params', {})
        return fetch_api_rest(url, method, headers, params, data)
    
    elif source_type == 'graphql':
        url = source_path
        query = kwargs.get('query', '')
        variables = kwargs.get('variables', {})
        headers = kwargs.get('headers', {})
        return fetch_api_graphql(url, query, variables, headers)
    
    elif source_type == 'sql':
        connection_string = source_path
        query = kwargs.get('query', 'SELECT * FROM table LIMIT 1')
        return read_sql_database(connection_string, query)
    
    raise ValueError(f"Type de source non supporté: {source_type}")


@task(name="ingestion", log_prints=True)
def ingestion_task(source_type: str, source_path: str, **kwargs):
    """Ingestion - Connecteur vers toutes les sources de données"""
    start_time = time.time()
    
    try:
        actual_source_type = detect_source_type(source_type, source_path)
        print(f"🔍 Type de source détecté: {actual_source_type}")
        
        actual_file_path = None
        if actual_source_type in ['csv', 'json', 'xml', 'excel', 'parquet', 'html']:
            actual_file_path = source_path if source_path.startswith('/') else f"/app/data/{source_path}"
        
        raw_data = read_data_from_source(
            source_type=actual_source_type,
            source_path=source_path,
            file_path=actual_file_path,
            **kwargs
        )
        
        print(f"📊 Données lues avec succès")
        
        raw_payload = {
            "source_type": actual_source_type,
            "source_path": source_path,
            "ingested_at": datetime.now().isoformat(),
            "data": raw_data,
        }
        
        doc_id = insert_raw_data({
            "source_type": actual_source_type,
            "source_path": source_path,
            "ingested_at": datetime.now().isoformat(),
            "step": "ingestion",
            "raw_payload": raw_payload,
            "status": "pending"
        })
        
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "ingestion",
            "duration_ms": duration_ms,
            "status": "success",
            "cpu_percent": get_cpu_usage(),
            "memory_mb": get_memory_mb(),
            "timestamp": datetime.now().isoformat()
        })
        
        return {
            "status": "success",
            "raw_payload": raw_payload,
            "document_id": doc_id,
            "source_type": actual_source_type,
            "duration_ms": round(duration_ms, 2)
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "ingestion",
            "duration_ms": duration_ms,
            "status": "error",
            "error": str(e)[:100],
            "timestamp": datetime.now().isoformat()
        })
        raise