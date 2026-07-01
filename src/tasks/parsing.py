"""Tâche de parsing pour Prefect"""
import time
import json
from datetime import datetime
from prefect import task
from ..utils.mongodb import save_metrics
from ..utils.metrics import get_cpu_usage, get_memory_mb


def extract_data_from_html(html_data):
    """Extrait et formate les données HTML"""
    result = {}
    
    if 'titles' in html_data and html_data['titles']:
        result['name'] = html_data['titles'][0]
    
    if 'tables' in html_data and html_data['tables']:
        table = html_data['tables'][0]
        if len(table) > 1:
            headers = table[0] if table else []
            values = table[1] if len(table) > 1 else []
            
            for i, header in enumerate(headers):
                header_lower = header.lower().strip()
                if i < len(values):
                    value = values[i]
                    if 'id' in header_lower or 'product' in header_lower:
                        result['id'] = value
                    elif 'name' in header_lower:
                        if 'name' not in result:
                            result['name'] = value
                    elif 'price' in header_lower or 'amount' in header_lower:
                        clean_price = value.replace('$', '').replace('€', '').replace(',', '').strip()
                        result['amount'] = clean_price
                    elif 'date' in header_lower:
                        result['date'] = value
                    elif 'country' in header_lower:
                        result['country'] = value
                    elif 'email' in header_lower:
                        result['email'] = value
    
    if 'name' in result and 'id' not in result:
        result['id'] = 'HTML001'
    
    if 'date' not in result:
        result['date'] = datetime.now().strftime('%Y-%m-%d')
    
    if 'email' not in result:
        result['email'] = 'no-email@example.com'
    
    return result


@task(name="parsing", log_prints=True)
def parsing_task(raw_data, format: str = "json"):
    """Parsing - CSV, Excel, JSON, HTML, Parquet"""
    start_time = time.time()
    
    try:
        print(f"📊 Parsing du format: {format}")
        
        if isinstance(raw_data, list):
            records = raw_data[0] if raw_data else {"empty": True, "count": 0}
        elif isinstance(raw_data, dict):
            records = raw_data
        else:
            records = {"value": raw_data}
        
        if format == 'html' or (isinstance(records, dict) and any(k in records for k in ['titles', 'paragraphs', 'tables'])):
            print("🔄 Détection de données HTML, extraction en cours...")
            records = extract_data_from_html(records)
        
        parsed_data = {
            "format": format,
            "records": records,
            "record_count": len(records) if isinstance(records, (dict, list)) else 1
        }
        
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "parsing",
            "duration_ms": duration_ms,
            "status": "success",
            "cpu_percent": get_cpu_usage(),
            "memory_mb": get_memory_mb(),
            "timestamp": datetime.now().isoformat()
        })
        
        return {
            "status": "success",
            "parsed_data": parsed_data,
            "duration_ms": round(duration_ms, 2)
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "parsing",
            "duration_ms": duration_ms,
            "status": "error",
            "error": str(e)[:100],
            "timestamp": datetime.now().isoformat()
        })
        raise