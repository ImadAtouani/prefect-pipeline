"""Script de déploiement des flows Prefect"""
import os
import sys
sys.path.insert(0, '/app')

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule
from datetime import timedelta

from src.flows.normalization_flow import normalization_flow
from src.flows.metrics_flow import metrics_flow


def deploy_normalization_flow():
    """Déploie le flow de normalisation"""
    deployment = Deployment.build_from_flow(
        flow=normalization_flow,
        name="normalization-pipeline",
        version="1.0.0",
        description="Pipeline de normalisation de données",
        parameters={
            "source_type": "csv",
            "source_path": "/app/data/sales_2024.csv"
        },
        work_pool_name="local-pool",
        tags=["data", "etl", "normalization"]
    )
    deployment.apply()
    print("✅ Déploiement 'normalization-pipeline' créé avec succès")


def deploy_metrics_flow():
    """Déploie le flow de métriques"""
    deployment = Deployment.build_from_flow(
        flow=metrics_flow,
        name="metrics-pipeline",
        version="1.0.0",
        description="Flow de métriques du pipeline",
        work_pool_name="local-pool",
        tags=["monitoring", "metrics", "observability"],
        schedule=IntervalSchedule(interval=timedelta(minutes=5))
    )
    deployment.apply()
    print("✅ Déploiement 'metrics-pipeline' créé avec succès")


if __name__ == "__main__":
    print("=" * 60)
    print("🚀 DÉPLOIEMENT DES FLOWS PRECEF")
    print("=" * 60)
    
    try:
        deploy_normalization_flow()
        deploy_metrics_flow()
        print("\n" + "=" * 60)
        print("✅ Tous les déploiements ont été créés avec succès !")
        print("📊 Accédez à l'interface Prefect: http://localhost:4200")
        print("=" * 60)
    except Exception as e:
        print(f"❌ Erreur lors du déploiement: {e}")
        sys.exit(1)