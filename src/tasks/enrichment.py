"""Tâche d'enrichissement pour Prefect"""
import time
from datetime import datetime
from prefect import task
from ..utils.mongodb import save_metrics
from ..utils.metrics import get_cpu_usage, get_memory_mb


@task(name="enrichment", log_prints=True)
def enrichment_task(validated_data: dict):
    """Enrichissement - métadonnées, source, horodatage"""
    start_time = time.time()
    
    try:
        if not isinstance(validated_data, dict):
            raise ValueError(f"validated_data is not a dict: {type(validated_data)}")
        
        enriched = validated_data.copy()
        enriched.update({
            "enriched_at": datetime.now().isoformat(),
            "pipeline_version": "1.0.0",
            "source_system": "prefect_pipeline",
            "processing_timestamp": datetime.now().timestamp(),
        })
        
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "enrichment",
            "duration_ms": duration_ms,
            "status": "success",
            "cpu_percent": get_cpu_usage(),
            "memory_mb": get_memory_mb(),
            "timestamp": datetime.now().isoformat()
        })
        
        return {
            "status": "success",
            "enriched_data": enriched,
            "duration_ms": round(duration_ms, 2)
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "enrichment",
            "duration_ms": duration_ms,
            "status": "error",
            "error": str(e)[:100],
            "timestamp": datetime.now().isoformat()
        })
        raise