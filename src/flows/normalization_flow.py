"""Flow principal de normalisation des données pour Prefect"""
from typing import Literal, Optional, Dict, Any
from prefect import flow
from prefect.artifacts import create_markdown_artifact
import json
from datetime import datetime

from src.tasks.ingestion import ingestion_task
from src.tasks.profiling import profiling_task
from src.tasks.parsing import parsing_task
from src.tasks.mapping import mapping_task
from src.tasks.cleaning import cleaning_task
from src.tasks.typing import typing_task
from src.tasks.standardization import standardization_task
from src.tasks.dedup import dedup_task
from src.tasks.validation import validation_task
from src.tasks.enrichment import enrichment_task
from src.tasks.mongodb_writer import mongodb_writer_task


@flow(
    name="Data Normalization Pipeline",
    log_prints=True,
    description="""
    Pipeline complet de normalisation de données en 12 étapes
    
    Sources supportées:
    - CSV, JSON, XML, Excel, Parquet, HTML
    - API REST, GraphQL
    - Bases de données SQL
    
    Étapes:
    1. Ingestion - Lecture des données depuis la source
    2. Profilage - Analyse des types et colonnes
    3. Parsing - Parsing selon le format
    4. Mapping - Mapping source → modèle cible
    5. Nettoyage - Trim, encodage, formats
    6. Typage - Date, nombre, booléen, enum
    7. Standardisation - Noms, unités, devises, pays
    8. Déduplication - Clé métier / hash
    9. Validation - Règles métier + schéma
    10. Enrichissement - Métadonnées, source, horodatage
    11. Écriture - Sauvegarde dans MongoDB (normalized ou rejected)
    12. Métriques - Envoi des métriques à l'OTEL Collector
    """
)
def normalization_flow(
    source_type: Literal["csv", "json", "xml", "excel", "parquet", "html", "api", "graphql", "sql"] = "csv",
    source_path: str = "/app/data/sales_2024.csv",
    format: Optional[str] = None,
    method: Literal["GET", "POST", "PUT", "DELETE"] = "GET",
    headers: str = "{}",
    params: str = "{}",
    data: str = "{}",
    query: str = "",
    variables: str = "{}"
) -> Dict[str, Any]:
    """
    Exécute le pipeline de normalisation complet.
    
    Args:
        source_type: Type de source de données
        source_path: Chemin du fichier, URL de l'API, ou chaîne de connexion SQL
        format: Format du fichier (pour les fichiers)
        method: Méthode HTTP pour API REST
        headers: Headers HTTP (format JSON)
        params: Paramètres de requête (format JSON)
        data: Corps de la requête (format JSON)
        query: Requête GraphQL ou SQL
        variables: Variables GraphQL (format JSON)
    
    Returns:
        Résultat du pipeline avec statut et données normalisées
    """
    print("=" * 60)
    print("🚀 PIPELINE DE NORMALISATION - PRECEF")
    print(f"📁 Source: {source_type} - {source_path}")
    print("=" * 60)
    
    # === ÉTAPE 1: INGESTION ===
    print("\n📥 ÉTAPE 1: INGESTION")
    result_ingestion = ingestion_task(
        source_type=source_type,
        source_path=source_path,
        format=format,
        method=method,
        headers=headers,
        params=params,
        data=data,
        query=query,
        variables=variables
    )
    
    raw_data = result_ingestion["raw_payload"]["data"]
    
    # === ÉTAPE 2: PROFILAGE ===
    print("\n📊 ÉTAPE 2: PROFILAGE")
    result_profiling = profiling_task(raw_data)
    
    # === ÉTAPE 3: PARSING ===
    print("\n🔍 ÉTAPE 3: PARSING")
    result_parsing = parsing_task(result_profiling, format=format or source_type)
    
    # === ÉTAPE 4: MAPPING ===
    print("\n🔄 ÉTAPE 4: MAPPING")
    result_mapping = mapping_task(result_parsing["parsed_data"])
    
    # === ÉTAPE 5: NETTOYAGE ===
    print("\n🧹 ÉTAPE 5: NETTOYAGE")
    result_cleaning = cleaning_task(result_mapping["mapped_data"])
    
    # === ÉTAPE 6: TYPAGE ===
    print("\n📝 ÉTAPE 6: TYPAGE")
    result_typing = typing_task(result_cleaning["cleaned_data"])
    
    # === ÉTAPE 7: STANDARDISATION ===
    print("\n📏 ÉTAPE 7: STANDARDISATION")
    result_standardization = standardization_task(result_typing["typed_data"])
    
    # === ÉTAPE 8: DÉDUPLICATION ===
    print("\n🔑 ÉTAPE 8: DÉDUPLICATION")
    result_dedup = dedup_task(result_standardization["standardized_data"])
    
    # === ÉTAPE 9: VALIDATION ===
    print("\n✅ ÉTAPE 9: VALIDATION")
    result_validation = validation_task(result_dedup["deduplicated_data"])
    
    # === ÉTAPE 10: ENRICHISSEMENT ET ÉCRITURE ===
    if result_validation["is_valid"]:
        print("\n📈 ÉTAPE 10: ENRICHISSEMENT (DATA VALIDE)")
        result_enrichment = enrichment_task(result_validation["validated_data"])
        
        # === ÉTAPE 11: ÉCRITURE NORMALISÉE ===
        print("\n💾 ÉTAPE 11: ÉCRITURE NORMALISÉE")
        result_write = mongodb_writer_task(
            data=result_enrichment["enriched_data"],
            collection="normalized_data"
        )
        final_data = result_enrichment["enriched_data"]
        status = "success"
    else:
        print("\n❌ ÉTAPE 10: DONNÉES REJETÉES")
        
        rejected_doc = {
            "raw_data": result_ingestion["raw_payload"],
            "errors": result_validation["errors"],
            "warnings": result_validation["warnings"],
            "dedup_key": result_dedup["dedup_key"],
            "rejected_at": datetime.now().isoformat()
        }
        
        # === ÉTAPE 11: ÉCRITURE REJETÉE ===
        print("\n💾 ÉTAPE 11: ÉCRITURE REJETÉE")
        result_write = mongodb_writer_task(
            data=rejected_doc,
            collection="rejected_data"
        )
        final_data = None
        status = "rejected"
    
    # === ÉTAPE 12: RÉSULTAT FINAL ===
    result = {
        "status": status,
        "source_type": source_type,
        "source_path": source_path,
        "ingestion_duration_ms": result_ingestion["duration_ms"],
        "profile": result_profiling["profile"],
        "validation_errors": result_validation["errors"],
        "validation_warnings": result_validation["warnings"],
        "write_result": result_write,
        "final_data": final_data,
        "pipeline_version": "1.0.0",
        "execution_time": datetime.now().isoformat()
    }
    
    # Créer un artefact Markdown pour la traçabilité
    markdown_content = f"""
    # Résultat du Pipeline de Normalisation
    
    ## Informations générales
    - **Statut**: {status}
    - **Source**: {source_type} - {source_path}
    - **Durée ingestion**: {result_ingestion["duration_ms"]:.2f} ms
    - **Exécution**: {datetime.now().isoformat()}
    
    ## Profilage
    - **Colonnes**: {result_profiling["profile"]["column_count"]}
    - **Noms des colonnes**: {", ".join(result_profiling["profile"]["columns"])}
    
    ## Validation
    - **Valide**: {result_validation["is_valid"]}
    - **Erreurs**: {len(result_validation["errors"])}
    - **Avertissements**: {len(result_validation["warnings"])}
    
    ## Écriture
    - **Collection**: {result_write["collection"]}
    - **ID**: {result_write["inserted_id"]}
    """
    
    create_markdown_artifact(
        key="pipeline-result",
        markdown=markdown_content,
        description=f"Résultat du pipeline pour {source_path}"
    )
    
    print("\n" + "=" * 60)
    print(f"✅ PIPELINE TERMINÉ - STATUT: {status.upper()}")
    print(f"📄 Document ID: {result_write['inserted_id']}")
    print("=" * 60)
    
    return result