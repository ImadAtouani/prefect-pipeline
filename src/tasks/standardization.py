"""Tâche de standardisation pour Prefect"""
import time
import json
from datetime import datetime
from prefect import task
from ..utils.mongodb import save_metrics
from ..utils.metrics import get_cpu_usage, get_memory_mb


@task(name="standardization", log_prints=True)
def standardization_task(typed_data: dict):
    """Standardisation - noms, unités, devises, pays"""
    start_time = time.time()
    
    try:
        if not isinstance(typed_data, dict):
            raise ValueError(f"typed_data is not a dict: {type(typed_data)}")
        
        standardized = typed_data.copy()
        std_stats = {
            "countries_normalized": 0,
            "currencies_converted": 0,
            "units_normalized": 0,
        }
        
        country_mapping = {
            "FR": "France", "DE": "Germany", "US": "United States",
            "UK": "United Kingdom", "CA": "Canada", "ES": "Spain",
            "IT": "Italy", "JP": "Japan", "BR": "Brazil", "AU": "Australia",
        }
        
        if "country" in standardized:
            if standardized["country"] in country_mapping:
                standardized["country_name"] = country_mapping[standardized["country"]]
                std_stats["countries_normalized"] += 1
        
        if "amount" in standardized:
            standardized["amount_usd"] = standardized["amount"] * 1.08
            std_stats["currencies_converted"] += 1
        
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "standardization",
            "duration_ms": duration_ms,
            "status": "success",
            "cpu_percent": get_cpu_usage(),
            "memory_mb": get_memory_mb(),
            "timestamp": datetime.now().isoformat()
        })
        
        return {
            "status": "success",
            "standardized_data": standardized,
            "standardization_stats": std_stats,
            "duration_ms": round(duration_ms, 2)
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "standardization",
            "duration_ms": duration_ms,
            "status": "error",
            "error": str(e)[:100],
            "timestamp": datetime.now().isoformat()
        })
        raise