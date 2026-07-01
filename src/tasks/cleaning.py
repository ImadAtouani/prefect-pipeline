"""Tâche de nettoyage pour Prefect"""
import time
import json
from datetime import datetime
from prefect import task
from ..utils.mongodb import save_metrics
from ..utils.metrics import get_cpu_usage, get_memory_mb


@task(name="cleaning", log_prints=True)
def cleaning_task(mapped_data: dict):
    """Nettoyage - trim, encodage, formats"""
    start_time = time.time()
    
    try:
        if not isinstance(mapped_data, dict):
            raise ValueError(f"mapped_data is not a dict: {type(mapped_data)}")
        
        cleaned = {}
        cleaning_stats = {"trimmed": 0, "encoded": 0, "unchanged": 0}
        
        for key, value in mapped_data.items():
            if isinstance(value, str):
                trimmed = value.strip()
                if trimmed != value:
                    cleaning_stats["trimmed"] += 1
                
                encoded = trimmed.encode("utf-8").decode("utf-8")
                if encoded != trimmed:
                    cleaning_stats["encoded"] += 1
                else:
                    cleaning_stats["unchanged"] += 1
                
                cleaned[key] = encoded
            else:
                cleaned[key] = value
                cleaning_stats["unchanged"] += 1
        
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "cleaning",
            "duration_ms": duration_ms,
            "status": "success",
            "cpu_percent": get_cpu_usage(),
            "memory_mb": get_memory_mb(),
            "timestamp": datetime.now().isoformat()
        })
        
        return {
            "status": "success",
            "cleaned_data": cleaned,
            "cleaning_stats": cleaning_stats,
            "duration_ms": round(duration_ms, 2)
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "cleaning",
            "duration_ms": duration_ms,
            "status": "error",
            "error": str(e)[:100],
            "timestamp": datetime.now().isoformat()
        })
        raise