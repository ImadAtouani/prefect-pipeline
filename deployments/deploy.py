"""Script de déploiement des flows Prefect"""
import sys
sys.path.insert(0, '/app')

from datetime import timedelta

from prefect import flow

SOURCE = "/app"


def deploy_normalization_flow():
    """Déploie le flow de normalisation"""
    normalization_flow = flow.from_source(
        source=SOURCE,
        entrypoint="src/flows/normalization_flow.py:normalization_flow",
    )
    normalization_flow.deploy(
        name="normalization-pipeline",
        work_pool_name="local-pool",
        version="1.0.0",
        description="Pipeline de normalisation de données",
        parameters={
            "source_type": "csv",
            "source_path": "/app/data/sales_2024.csv"
        },
        tags=["data", "etl", "normalization"],
    )
    print("✅ Déploiement 'normalization-pipeline' créé avec succès")


def deploy_metrics_flow():
    """Déploie le flow de métriques"""
    metrics_flow = flow.from_source(
        source=SOURCE,
        entrypoint="src/flows/metrics_flow.py:metrics_flow",
    )
    metrics_flow.deploy(
        name="metrics-pipeline",
        work_pool_name="local-pool",
        version="1.0.0",
        description="Flow de métriques du pipeline",
        tags=["monitoring", "metrics", "observability"],
        interval=timedelta(minutes=5),
    )
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