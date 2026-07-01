"""Flow de métriques pour Prefect"""
from prefect import flow
from prefect.artifacts import create_markdown_artifact
from datetime import datetime

from ..utils.mongodb import get_collection
from ..utils.metrics import send_metric


@flow(
    name="Pipeline Metrics",
    log_prints=True,
    description="Envoi des métriques du pipeline à l'OTEL Collector"
)
def metrics_flow():
    """Met à jour toutes les métriques du pipeline"""
    
    try:
        print("📊 Envoi des métriques du pipeline...")
        
        raw_collection = get_collection("raw_data")
        norm_collection = get_collection("normalized_data")
        rej_collection = get_collection("rejected_data")
        metrics_collection = get_collection("script_metrics")
        
        # === 1. MÉTRIQUES DE COMPTAGE ===
        raw = raw_collection.count_documents({})
        norm = norm_collection.count_documents({})
        rej = rej_collection.count_documents({})
        pending = raw_collection.count_documents({"status": "pending"})
        
        print(f"📊 raw={raw}, normalized={norm}, rejected={rej}")
        
        send_metric("prefect.raw.data.total", raw, "1", "Total raw data")
        send_metric("prefect.normalized.data.total", norm, "1", "Total normalized data")
        send_metric("prefect.rejected.data.total", rej, "1", "Total rejected data")
        send_metric("prefect.raw.pending.total", pending, "1", "Pending data")
        
        # === 2. MÉTRIQUES DE LATENCE ===
        scripts = ["ingestion", "profiling", "parsing", "mapping", "cleaning", 
                   "typing", "standardization", "dedup", "validation", "enrichment", "mongodb_writer"]
        
        for script in scripts:
            last = metrics_collection.find_one(
                {"script": script},
                sort=[("timestamp", -1)]
            )
            if last:
                duration = last.get("duration_ms", 0)
                send_metric(
                    "prefect.latency.last",
                    duration,
                    "ms",
                    f"Last execution duration for {script}",
                    {"script": script}
                )
        
        # === 3. MÉTRIQUES D'ERREURS ===
        for script in scripts:
            errors = metrics_collection.count_documents({
                "script": script,
                "status": "error"
            })
            successes = metrics_collection.count_documents({
                "script": script,
                "status": "success"
            })
            
            send_metric(
                "prefect.errors.total",
                errors,
                "1",
                f"Total errors for {script}",
                {"script": script}
            )
            send_metric(
                "prefect.success.total",
                successes,
                "1",
                f"Total successes for {script}",
                {"script": script}
            )
            
            total = errors + successes
            if total > 0:
                error_rate = (errors / total) * 100
                send_metric(
                    "prefect.error.rate",
                    round(error_rate, 2),
                    "%",
                    f"Error rate for {script}",
                    {"script": script}
                )
        
        # === 4. MÉTRIQUES CPU ===
        for script in scripts:
            last = metrics_collection.find_one(
                {"script": script},
                sort=[("timestamp", -1)]
            )
            if last and "cpu_percent" in last:
                send_metric(
                    "prefect.cpu.percent",
                    last["cpu_percent"],
                    "%",
                    f"CPU usage for {script}",
                    {"script": script}
                )
        
        # === 5. MÉTRIQUES MÉMOIRE ===
        for script in scripts:
            last = metrics_collection.find_one(
                {"script": script},
                sort=[("timestamp", -1)]
            )
            if last and "memory_mb" in last:
                send_metric(
                    "prefect.memory.mb",
                    last["memory_mb"],
                    "MB",
                    f"Memory usage for {script}",
                    {"script": script}
                )
        
        # Créer un artefact
        create_markdown_artifact(
            key="metrics-summary",
            markdown=f"""
            # Métriques du Pipeline Prefect
            
            ## Comptage
            - **Données brutes**: {raw}
            - **Données normalisées**: {norm}
            - **Données rejetées**: {rej}
            
            ## Exécution
            - **Timestamp**: {datetime.now().isoformat()}
            - **Status**: ✅ Succès
            """,
            description="Synthèse des métriques du pipeline"
        )
        
        print("✅ Métriques envoyées avec succès !")
        return {"status": "success", "metrics_sent": True}
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return {"status": "error", "message": str(e)}