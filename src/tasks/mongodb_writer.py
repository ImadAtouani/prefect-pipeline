"""Tâche d'écriture MongoDB pour Prefect"""
import time
import json
from typing import Literal, Dict, Any
from datetime import datetime
from prefect import task
from ..utils.mongodb import insert_normalized_data, insert_rejected_data, save_metrics
from ..utils.metrics import get_cpu_usage, get_memory_mb


@task(name="mongodb_writer", log_prints=True)
def mongodb_writer_task(
    data: Dict[str, Any],
    collection: Literal["raw_data", "normalized_data", "rejected_data"] = "normalized_data"
):
    """Écriture dans MongoDB"""
    start_time = time.time()
    
    try:
        document = data.copy() if isinstance(data, dict) else {"data": data}
        document["written_at"] = datetime.now().isoformat()
        document["_collection"] = collection
        
        if collection == "normalized_data":
            inserted_id = insert_normalized_data(document)
        elif collection == "rejected_data":
            inserted_id = insert_rejected_data(document)
        else:
            raise ValueError(f"Collection non supportée: {collection}")
        
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "mongodb_writer",
            "duration_ms": duration_ms,
            "status": "success",
            "cpu_percent": get_cpu_usage(),
            "memory_mb": get_memory_mb(),
            "timestamp": datetime.now().isoformat()
        })
        
        return {
            "status": "success",
            "inserted_id": inserted_id,
            "collection": collection,
            "duration_ms": round(duration_ms, 2)
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        save_metrics({
            "script": "mongodb_writer",
            "duration_ms": duration_ms,
            "status": "error",
            "error": str(e)[:100],
            "timestamp": datetime.now().isoformat()
        })
        raise