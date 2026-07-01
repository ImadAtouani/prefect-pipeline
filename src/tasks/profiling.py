"""Tâche de profilage pour Prefect"""
import time
import json
from datetime import datetime
from prefect import task
from ..utils.mongodb import save_metrics
from ..utils.metrics import get_cpu_usage, get_memory_mb


@task(name="profiling", log_prints=True)
def profiling_task(raw_data):
    """Profilage - Analyse des types, colonnes, valeurs nulles"""
    start_time = time.time()
    
    try:
        print(f"📊 Profilage des données")
        
        if isinstance(raw_data, list):
            if len(raw_data) > 0 and isinstance(raw_data[0], dict):
                data = raw_data[0]
            else:
                data = {"items": raw_data, "count": len(raw_data)}
        elif isinstance(raw_data, dict):
            data = raw_data
        else:
            data = {"value": raw_data}
        
        if isinstance(data, dict):
            column_count = len(data)
            data_types = {k: type(v).__name__ for k, v in data.items()}
            null_values = {k: v is None for k, v in data.items()}
            columns = list(data.keys())
        else:
            column_count = 1
            data_types = {"value_type": type(data).__name__}
            null_values = {"is_null": data is None}
            columns = ["value"]
        
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "profiling",
            "duration_ms": duration_ms,
            "status": "success",
            "cpu_percent": get_cpu_usage(),
            "memory_mb": get_memory_mb(),
            "timestamp": datetime.now().isoformat()
        })
        
        return {
            "status": "success",
            "profile": {
                "column_count": column_count,
                "data_types": data_types,
                "null_values": null_values,
                "columns": columns,
            },
            "raw_data": raw_data,
            "duration_ms": round(duration_ms, 2)
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "profiling",
            "duration_ms": duration_ms,
            "status": "error",
            "error": str(e)[:100],
            "timestamp": datetime.now().isoformat()
        })
        raise