"""Tâche de typage pour Prefect"""
import time
import json
from datetime import datetime
from prefect import task
from ..utils.mongodb import save_metrics
from ..utils.metrics import get_cpu_usage, get_memory_mb


@task(name="typing", log_prints=True)
def typing_task(cleaned_data: dict, date_format: str = "%Y-%m-%d"):
    """Typage - date, nombre, booléen, enum"""
    start_time = time.time()
    
    try:
        if not isinstance(cleaned_data, dict):
            raise ValueError(f"cleaned_data is not a dict: {type(cleaned_data)}")
        
        typed_data = {}
        typing_stats = {
            "typed_as_date": 0,
            "typed_as_number": 0,
            "typed_as_bool": 0,
            "typed_as_enum": 0,
            "unchanged": 0,
            "errors": 0,
        }
        
        enum_values = ["FR", "DE", "US", "UK", "CA", "ES", "IT", "JP", "BR", "AU"]
        
        for key, value in cleaned_data.items():
            try:
                if key in ["date", "transaction_date", "birth_date"] and isinstance(value, str):
                    try:
                        typed_data[key] = datetime.strptime(value, date_format).isoformat()
                        typing_stats["typed_as_date"] += 1
                    except ValueError:
                        typed_data[key] = value
                        typing_stats["unchanged"] += 1
                
                elif key in ["amount", "price", "quantity", "value"]:
                    if isinstance(value, (int, float)):
                        typed_data[key] = float(value)
                        typing_stats["typed_as_number"] += 1
                    elif isinstance(value, str):
                        try:
                            clean_value = value.replace("€", "").replace("$", "").replace(",", "").strip()
                            typed_data[key] = float(clean_value)
                            typing_stats["typed_as_number"] += 1
                        except ValueError:
                            typed_data[key] = value
                            typing_stats["unchanged"] += 1
                    else:
                        typed_data[key] = value
                        typing_stats["unchanged"] += 1
                
                elif key in ["active", "enabled", "verified", "is_valid"]:
                    if isinstance(value, bool):
                        typed_data[key] = bool(value)
                        typing_stats["typed_as_bool"] += 1
                    elif isinstance(value, str):
                        if value.lower() in ["true", "yes", "1"]:
                            typed_data[key] = True
                            typing_stats["typed_as_bool"] += 1
                        elif value.lower() in ["false", "no", "0"]:
                            typed_data[key] = False
                            typing_stats["typed_as_bool"] += 1
                        else:
                            typed_data[key] = value
                            typing_stats["unchanged"] += 1
                    else:
                        typed_data[key] = value
                        typing_stats["unchanged"] += 1
                
                elif key in ["country", "country_code", "status"]:
                    if isinstance(value, str) and value in enum_values:
                        typed_data[key] = value
                        typing_stats["typed_as_enum"] += 1
                    else:
                        typed_data[key] = value
                        typing_stats["unchanged"] += 1
                
                else:
                    typed_data[key] = value
                    typing_stats["unchanged"] += 1
            
            except Exception as e:
                typing_stats["errors"] += 1
                typed_data[key] = value
        
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "typing",
            "duration_ms": duration_ms,
            "status": "success",
            "cpu_percent": get_cpu_usage(),
            "memory_mb": get_memory_mb(),
            "timestamp": datetime.now().isoformat()
        })
        
        return {
            "status": "success" if typing_stats["errors"] == 0 else "partial",
            "typed_data": typed_data,
            "typing_stats": typing_stats,
            "duration_ms": round(duration_ms, 2)
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "typing",
            "duration_ms": duration_ms,
            "status": "error",
            "error": str(e)[:100],
            "timestamp": datetime.now().isoformat()
        })
        raise