"""Tâche de déduplication pour Prefect"""
import time
import hashlib
from datetime import datetime
from prefect import task
from ..utils.mongodb import save_metrics
from ..utils.metrics import get_cpu_usage, get_memory_mb


@task(name="dedup", log_prints=True)
def dedup_task(standardized_data: dict):
    """Déduplication - clé métier / hash"""
    start_time = time.time()
    
    try:
        if not isinstance(standardized_data, dict):
            raise ValueError(f"standardized_data is not a dict: {type(standardized_data)}")
        
        data_string = str(sorted(standardized_data.items()))
        timestamp = datetime.now().isoformat()
        unique_string = f"{data_string}_{timestamp}_{id(standardized_data)}"
        dedup_key = hashlib.md5(unique_string.encode()).hexdigest()
        
        standardized_data_with_key = standardized_data.copy()
        standardized_data_with_key["dedup_key"] = dedup_key
        
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "dedup",
            "duration_ms": duration_ms,
            "status": "success",
            "cpu_percent": get_cpu_usage(),
            "memory_mb": get_memory_mb(),
            "timestamp": datetime.now().isoformat()
        })
        
        return {
            "status": "success",
            "deduplicated_data": standardized_data_with_key,
            "dedup_key": dedup_key,
            "duration_ms": round(duration_ms, 2)
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "dedup",
            "duration_ms": duration_ms,
            "status": "error",
            "error": str(e)[:100],
            "timestamp": datetime.now().isoformat()
        })
        raise