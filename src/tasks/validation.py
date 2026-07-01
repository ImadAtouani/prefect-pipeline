"""Tâche de validation pour Prefect"""
import time
from datetime import datetime
from prefect import task
from ..utils.mongodb import save_metrics
from ..utils.metrics import get_cpu_usage, get_memory_mb


@task(name="validation", log_prints=True)
def validation_task(deduplicated_data: dict):
    """Validation - règles métier + schéma"""
    start_time = time.time()
    
    try:
        if not isinstance(deduplicated_data, dict):
            raise ValueError(f"deduplicated_data is not a dict: {type(deduplicated_data)}")
        
        errors = []
        warnings = []
        
        if "user_id" not in deduplicated_data:
            errors.append("Missing required field: user_id")
        
        if "email_address" not in deduplicated_data:
            errors.append("Missing required field: email_address")
        
        if "amount" in deduplicated_data:
            if deduplicated_data["amount"] < 0:
                errors.append(f"Amount must be positive: {deduplicated_data['amount']}")
            elif deduplicated_data["amount"] > 10000:
                warnings.append(f"Amount is very high: {deduplicated_data['amount']}")
        
        if "email_address" in deduplicated_data:
            email = deduplicated_data["email_address"]
            if "@" not in email or "." not in email:
                errors.append(f"Invalid email format: {email}")
        
        if "country_code" in deduplicated_data:
            valid_countries = ["FR", "DE", "US", "UK", "CA", "ES", "IT", "JP", "BR", "AU"]
            if deduplicated_data["country_code"] not in valid_countries:
                warnings.append(f"Unknown country code: {deduplicated_data['country_code']}")
        
        is_valid = len(errors) == 0
        
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "validation",
            "duration_ms": duration_ms,
            "status": "success",
            "cpu_percent": get_cpu_usage(),
            "memory_mb": get_memory_mb(),
            "timestamp": datetime.now().isoformat()
        })
        
        return {
            "status": "valid" if is_valid else "rejected",
            "validated_data": deduplicated_data if is_valid else None,
            "errors": errors,
            "warnings": warnings,
            "is_valid": is_valid,
            "duration_ms": round(duration_ms, 2)
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "validation",
            "duration_ms": duration_ms,
            "status": "error",
            "error": str(e)[:100],
            "timestamp": datetime.now().isoformat()
        })
        raise