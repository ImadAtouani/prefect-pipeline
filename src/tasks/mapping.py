"""Tâche de mapping pour Prefect"""
import time
import json
from datetime import datetime
from prefect import task
from ..utils.mongodb import save_metrics
from ..utils.metrics import get_cpu_usage, get_memory_mb


@task(name="mapping", log_prints=True)
def mapping_task(parsed_data: dict):
    """Mapping - champs source → modèle cible"""
    start_time = time.time()
    
    try:
        if isinstance(parsed_data, dict) and "records" in parsed_data:
            records = parsed_data["records"]
        else:
            records = parsed_data
        
        if isinstance(records, list):
            if not records:
                raise ValueError("Records list is empty")
            records = records[0]
        elif not isinstance(records, dict):
            records = {"value": records}
        
        mapping_rules = {
            "id": "user_id",
            "name": "full_name",
            "amount": "amount",
            "date": "transaction_date",
            "country": "country_code",
            "email": "email_address",
        }
        
        mapped_data = {}
        for source, target in mapping_rules.items():
            if source in records:
                mapped_data[target] = records[source]
        
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "mapping",
            "duration_ms": duration_ms,
            "status": "success",
            "cpu_percent": get_cpu_usage(),
            "memory_mb": get_memory_mb(),
            "timestamp": datetime.now().isoformat()
        })
        
        return {
            "status": "success",
            "mapped_data": mapped_data,
            "duration_ms": round(duration_ms, 2)
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "mapping",
            "duration_ms": duration_ms,
            "status": "error",
            "error": str(e)[:100],
            "timestamp": datetime.now().isoformat()
        })
        raise