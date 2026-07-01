# Prefect Pipeline - Orchestration de Normalisation de Données

## 📋 Description

Ce projet implémente un pipeline complet de normalisation de données orchestré par **Prefect**, avec :
- 🔄 Pipeline ETL en 12 étapes (ingestion → écriture → métriques)
- 📊 Stockage dans MongoDB (raw / normalized / rejected)
- 📈 Observabilité complète avec OpenTelemetry + Prometheus + Tempo + Grafana
- 🎯 Interface Web Prefect pour la gestion et le monitoring des flows

Le pipeline traite des données provenant de multiples sources (CSV, Excel, JSON, HTML, API, Parquet, XML) et les normalise selon un modèle cible défini.

---

## 🏗️ Architecture

```
Sources (externes)
    ↓
[Prefect - Tasks Python]
    ├── ingestion.py          # Connecteurs sources (CSV, JSON, XML, Excel, Parquet, HTML, API, GraphQL, SQL)
    ├── profiling.py          # Analyse des types, colonnes, valeurs nulles
    ├── parsing.py            # Parsing des formats
    ├── mapping.py            # Mapping source → modèle cible
    ├── cleaning.py           # Nettoyage des données
    ├── typing.py             # Typage des champs
    ├── standardization.py    # Standardisation
    ├── dedup.py              # Déduplication
    ├── validation.py         # Règles métier + schéma
    ├── enrichment.py         # Enrichissement
    ├── mongodb_writer.py     # Écriture MongoDB
    └── send_metrics.py       # Envoi des métriques à l'OTEL Collector (via metrics_flow)
    ↓                    ↓                    ↓
[MongoDB]          [MongoDB]          [MongoDB]
raw_data           normalized_data   rejected_data
    ↓                    ↓                    ↓
[OpenTelemetry Collector]
    ↓                    ↓
[Prometheus]        [Tempo]
(métriques)         (traces)
    ↓                    ↓
[Grafana - Dashboards]
```

### Étapes du Pipeline

| # | Étape | Task | Description |
|---|-------|------|-------------|
| 1 | **Ingestion** | `ingestion.py` | Connecteurs sources (CSV, JSON, XML, Excel, Parquet, HTML, API, GraphQL, SQL) |
| 2 | **Profilage** | `profiling.py` | Analyse des types, colonnes, valeurs nulles |
| 3 | **Parsing** | `parsing.py` | Parse CSV, Excel, JSON, HTML, Parquet, XML |
| 4 | **Mapping** | `mapping.py` | Mapping champs source → modèle cible |
| 5 | **Nettoyage** | `cleaning.py` | Trim, encodage, formats |
| 6 | **Typage** | `typing.py` | Cast date, nombre, booléen, enum |
| 7 | **Standardisation** | `standardization.py` | Normalisation noms, unités, devises, pays |
| 8 | **Déduplication** | `dedup.py` | Clé métier / hash |
| 9 | **Validation** | `validation.py` | Règles métier + schéma |
| 10 | **Enrichissement** | `enrichment.py` | Métadonnées, source, horodatage |
| 11 | **Écriture** | `mongodb_writer.py` | Sauvegarde dans MongoDB (normalized ou rejected) |
| 12 | **Métriques** | `metrics_flow.py` | Envoi des métriques à l'OTEL Collector |

---

## 📁 Structure du Projet

```
prefect-pipeline/
│
├── docker-compose.yml          # Orchestration complète
├── Dockerfile                  # Image Docker personnalisée
├── requirements.txt            # Dépendances Python
├── .env                        # Variables d'environnement
│
├── src/
│   ├── __init__.py
│   ├── flows/
│   │   ├── __init__.py
│   │   ├── normalization_flow.py    # Pipeline principal
│   │   └── metrics_flow.py          # Métriques OTEL
│   │
│   ├── tasks/
│   │   ├── __init__.py
│   │   ├── ingestion.py
│   │   ├── profiling.py
│   │   ├── parsing.py
│   │   ├── mapping.py
│   │   ├── cleaning.py
│   │   ├── typing.py
│   │   ├── standardization.py
│   │   ├── dedup.py
│   │   ├── validation.py
│   │   ├── enrichment.py
│   │   └── mongodb_writer.py
│   │
│   └── utils/
│       ├── __init__.py
│       ├── mongodb.py
│       └── metrics.py
│
├── deployments/
│   └── deploy.py               # Déploiement des flows
│
├── data/                       # Fichiers de données sources
│   ├── sales_2024.csv
│   ├── products.json
│   ├── data.xml
│   ├── inventory.xlsx
│   ├── data.parquet
│   └── page.html
│
├── grafana/                    # Configuration Grafana
│   ├── datasources.yml
│   ├── dashboards.yml
│   └── dashboards/
│       └── pipeline_overview.json
│
├── mongo-init/                 # Initialisation MongoDB
│   └── init.js
│
├── otel-collector-config.yml   # Configuration OpenTelemetry
├── prometheus.yml              # Configuration Prometheus
├── tempo.yml                   # Configuration Tempo
│
└── README.md
```

---

## 🚀 Installation et Lancement

### Prérequis
- **Docker** & **Docker Compose** (v2.0+)
- **8GB RAM** minimum
- **Ports disponibles** : 4200, 27017, 3000, 9090, 3200, 4317, 4318, 55680, 8890

### 1. Créer et préparer le projet

```bash
# Créer le dossier du projet
mkdir prefect-pipeline
cd prefect-pipeline

# Créer les dossiers nécessaires
mkdir -p src/{flows,tasks,utils} data grafana/dashboards mongo-init deployments
```

### 2. Configuration des fichiers

Copiez tous les fichiers suivants dans la structure indiquée :
- `docker-compose.yml`
- `Dockerfile`
- `requirements.txt`
- `.env`
- `otel-collector-config.yml`
- `prometheus.yml`
- `tempo.yml`
- Les fichiers de données dans `data/`
- Tous les scripts Python dans `src/`
- Les fichiers Grafana dans `grafana/`
- Les fichiers MongoDB dans `mongo-init/`

### 3. Construction de l'image Docker

```bash
# Construire l'image personnalisée
docker-compose build
```

### 4. Lancer le stack complet

```bash
# Démarrer tous les services en arrière-plan
docker-compose up -d

# Vérifier que tous les services sont up
docker-compose ps

# Voir les logs en temps réel
docker-compose logs -f

# Voir les logs d'un service spécifique
docker-compose logs -f prefect-server
docker-compose logs -f mongodb
```

### 5. Vérifier l'état des services

```bash
# Vérifier la santé de chaque service
docker-compose ps
```

**Résultat attendu :**
```
NAME                                STATE    PORTS
prefect-postgres-1         Up (healthy)   5432/tcp
prefect-redis-1            Up (healthy)   6379/tcp
prefect-prefect-server-1   Up (healthy)   0.0.0.0:4200->4200/tcp
prefect-prefect-services-1 Up             4200/tcp
prefect-prefect-worker-1   Up             4200/tcp
prefect-mongodb-1          Up (healthy)   0.0.0.0:27017->27017/tcp
prefect-otel_collector-1   Up             0.0.0.0:4317->4317/tcp, 0.0.0.0:4318->4318/tcp, 0.0.0.0:8890->8890/tcp
prefect-prometheus-1       Up             0.0.0.0:9090->9090/tcp
prefect-tempo-1            Up             0.0.0.0:3200->3200/tcp, 0.0.0.0:55680->55680/tcp
prefect-grafana-1          Up             0.0.0.0:3000->3000/tcp
```

### 6. Déployer les flows dans Prefect

```bash
# Déployer les flows
docker exec -it prefect-prefect-worker-1 python /app/deployments/deploy.py

# Vérifier les déploiements
docker exec -it prefect-prefect-worker-1 prefect deployment ls
```

---

## 🔗 Accès aux Services

| Service | URL | Identifiants |
|---------|-----|--------------|
| **Prefect UI** | http://localhost:4200 | - |
| **MongoDB** | mongodb://admin:changeme@localhost:27017 | admin / changeme |
| **Grafana** | http://localhost:3000 | admin / admin |
| **Prometheus** | http://localhost:9090 | - |
| **Tempo** | http://localhost:3200/status | - |
| **OTEL Collector Metrics** | http://localhost:8890/metrics | - |

> **Notes :**
> - **Prefect UI** est accessible sans authentification en local
> - **Tempo** n'a pas d'interface web sur la racine (`/`). Utilisez `/status` ou `/ready` pour vérifier l'état du service.
> - **OTEL Collector Metrics** peut être vide au premier lancement. Les métriques apparaîtront après la première exécution du Flow Prefect.

### Premier accès à Prefect

1. Ouvrez http://localhost:4200 dans votre navigateur
2. L'interface Prefect s'affiche sans authentification requise
3. Vous pouvez voir les déploiements dans l'onglet **"Deployments"**

---

## 🧪 Tester le Pipeline

### Vérification de santé des services

```bash
# Tester que tous les services répondent
curl -I http://localhost:8890/metrics   # OTEL Collector (200 OK)
curl http://localhost:3200/status       # Tempo ({"status":"running"})
curl http://localhost:3200/ready        # Tempo (ready)
curl http://localhost:9090/api/v1/query?query=up  # Prometheus
curl http://localhost:3000              # Grafana
curl http://localhost:4200/api/health   # Prefect Server
```

### Méthode 1 : Via l'interface Prefect (Recommandé)

#### Étape 1 : Accéder à l'interface

1. Connectez-vous à Prefect (http://localhost:4200)
2. Dans le menu, allez dans **"Deployments"**
3. Vous verrez le déploiement **"normalization-pipeline"**

#### Étape 2 : Exécuter le Flow

1. Cliquez sur **"normalization-pipeline"**
2. Cliquez sur le bouton **"Custom Run"** (pas "Quick Run", qui relance avec les derniers paramètres utilisés sans permettre de les modifier)
3. Remplissez les paramètres :
   - `source_type`: `csv` (ou autre)
   - `source_path`: `/app/data/sales_2024.csv`
4. Cliquez sur **"Run"**

#### Étape 3 : Suivre l'exécution

1. Allez dans **"Flow Runs"** dans le menu de gauche
2. Vous verrez l'exécution en cours ou terminée
3. Cliquez sur un run pour voir :
   - Les logs détaillés
   - La durée d'exécution
   - Les paramètres utilisés
   - Les artefacts générés

#### Étape 4 : Exécuter avec différents inputs

| Source | Parameters JSON |
|--------|-----------------|
| **CSV** | `{"source_type":"csv","source_path":"/app/data/sales_2024.csv"}` |
| **JSON** | `{"source_type":"json","source_path":"/app/data/products.json"}` |
| **XML** | `{"source_type":"xml","source_path":"/app/data/data.xml"}` |
| **Excel** | `{"source_type":"excel","source_path":"/app/data/inventory.xlsx"}` |
| **Parquet** | `{"source_type":"parquet","source_path":"/app/data/data.parquet"}` |
| **HTML** | `{"source_type":"html","source_path":"/app/data/page.html"}` |
| **API GET** | `{"source_type":"api","source_path":"https://jsonplaceholder.typicode.com/users/1","method":"GET"}` |
| **API GET avec params** | `{"source_type":"api","source_path":"https://jsonplaceholder.typicode.com/posts","method":"GET","params":"{\"userId\":1}"}` |
| **API POST** | `{"source_type":"api","source_path":"https://jsonplaceholder.typicode.com/posts","method":"POST","headers":"{\"Content-Type\":\"application/json\"}","data":"{\"title\":\"Test\",\"body\":\"Test content\",\"userId\":1}"}` |
| **GraphQL** | `{"source_type":"graphql","source_path":"https://rickandmortyapi.com/graphql","query":"query { characters(page:1) { results { id name status species } } }"}` |
| **SQL** | `{"source_type":"sql","source_path":"postgresql://user:password@localhost:5432/mydb","query":"SELECT * FROM users LIMIT 5"}` |

### Méthode 2 : Via l'API Prefect

```bash
# 1. Récupérer l'ID du déploiement
DEPLOYMENT_ID=$(curl -s http://localhost:4200/api/deployments | jq -r '.[] | select(.name=="normalization-pipeline") | .id')

# 2. Lancer une exécution CSV
curl -X POST "http://localhost:4200/api/deployments/$DEPLOYMENT_ID/run" \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"source_type": "csv", "source_path": "/app/data/sales_2024.csv"}}'

# 3. Lancer une exécution JSON
curl -X POST "http://localhost:4200/api/deployments/$DEPLOYMENT_ID/run" \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"source_type": "json", "source_path": "/app/data/products.json"}}'

# 4. Lancer une exécution HTML
curl -X POST "http://localhost:4200/api/deployments/$DEPLOYMENT_ID/run" \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"source_type": "html", "source_path": "/app/data/page.html"}}'
```

### Méthode 3 : Exécuter le flow de métriques

```bash
# Via l'API
curl -X POST "http://localhost:4200/api/deployments/metrics-pipeline/run" \
  -H "Content-Type: application/json" \
  -d '{}'
```

### Vérifier les résultats

```bash
# Connexion à MongoDB
docker exec -it prefect-mongodb-1 mongosh -u admin -p changeme

# Dans le shell MongoDB
use data_pipeline

# Voir les données brutes
db.raw_data.find().pretty()

# Voir les données normalisées
db.normalized_data.find().pretty()

# Voir les données rejetées
db.rejected_data.find().pretty()

# Voir les métriques d'exécution
db.script_metrics.find().sort({timestamp: -1}).limit(10).pretty()
```

### Vérifier les métriques

```bash
# Voir les métriques du pipeline (préfixe prefect_)
curl -s http://localhost:8890/metrics | grep "prefect"

# Résultat attendu :
# prefect_raw_data_total_ratio 5
# prefect_normalized_data_total_ratio 5
# prefect_rejected_data_total_ratio 0
# prefect_raw_pending_total_ratio 0
# prefect_raw_by_source_ratio{source_type="csv"} 1
# prefect_raw_by_source_ratio{source_type="json"} 1
# prefect_raw_by_source_ratio{source_type="xml"} 1
# prefect_raw_by_source_ratio{source_type="html"} 1
# prefect_latency_last_milliseconds{script="ingestion"} 13.92
# prefect_cpu_percent{script="ingestion"} 6.64
# prefect_memory_mb_MB{script="ingestion"} 28.86
# prefect_error_rate_percent{script="ingestion"} 0
```

---

## 📊 Observabilité

### Métriques disponibles

| Métrique | Description | Unité |
|----------|-------------|-------|
| `prefect_raw_data_total_ratio` | Nombre total de données brutes | 1 |
| `prefect_normalized_data_total_ratio` | Nombre total de données normalisées | 1 |
| `prefect_rejected_data_total_ratio` | Nombre total de données rejetées | 1 |
| `prefect_raw_by_source_ratio` | Répartition des données par source | 1 |
| `prefect_latency_last_milliseconds` | Dernière durée d'exécution par task | ms |
| `prefect_latency_avg_milliseconds` | Durée moyenne d'exécution par task | ms |
| `prefect_cpu_percent` | CPU par task | % |
| `prefect_cpu_avg_percent` | CPU moyenne par task | % |
| `prefect_memory_mb_MB` | Mémoire par task | MB |
| `prefect_memory_avg_MB` | Mémoire moyenne par task | MB |
| `prefect_errors_total_ratio` | Nombre d'erreurs par task | 1 |
| `prefect_success_total_ratio` | Nombre de succès par task | 1 |
| `prefect_error_rate_percent` | Taux d'erreur par task | % |
| `prefect_*_duration_ms` | Durée par task spécifique (ingestion, profiling, etc.) | ms |

### Dashboards Grafana

1. Connectez-vous à Grafana (http://localhost:3000)
   - User: `admin`
   - Password: `admin`

2. Allez dans **"Dashboards"** → **"Browse"**
3. Sélectionnez **"Prefect Pipeline - Vue Globale"**

Le dashboard affiche :
- **Statistiques** : Données brutes, normalisées, rejetées
- **Évolution des données** : Graphique temporel des 3 métriques principales
- **Répartition par source** : Diagramme circulaire des sources de données
- **Latence par tâche** : Dernière exécution et moyenne
- **Taux d'erreur** : Par script
- **CPU par tâche** : Utilisation CPU par script
- **Mémoire par tâche** : Utilisation mémoire par script
- **Détails des exécutions** : Tableau des durées par tâche

---

## 🧪 Tester l'Observabilité

### Tester l'OTEL Collector

```bash
# Voir toutes les métriques
curl -s http://localhost:8890/metrics | grep "prefect"

# Métriques de comptage
curl -s http://localhost:8890/metrics | grep "_data_total_ratio"

# Métriques de latence
curl -s http://localhost:8890/metrics | grep "latency"

# Métriques CPU
curl -s http://localhost:8890/metrics | grep "cpu"

# Métriques Mémoire
curl -s http://localhost:8890/metrics | grep "memory"

# Métriques d'erreurs
curl -s http://localhost:8890/metrics | grep "error"
```

### Tester Prometheus

```bash
# Données brutes
curl -s 'http://localhost:9090/api/v1/query?query=prefect_raw_data_total_ratio'

# Données normalisées
curl -s 'http://localhost:9090/api/v1/query?query=prefect_normalized_data_total_ratio'

# Données rejetées
curl -s 'http://localhost:9090/api/v1/query?query=prefect_rejected_data_total_ratio'

# Latence
curl -s 'http://localhost:9090/api/v1/query?query=prefect_latency_last_milliseconds'

# CPU
curl -s 'http://localhost:9090/api/v1/query?query=prefect_cpu_percent'

# Mémoire
curl -s 'http://localhost:9090/api/v1/query?query=prefect_memory_mb_MB'

# Taux d'erreur
curl -s 'http://localhost:9090/api/v1/query?query=prefect_error_rate_percent'
```

**Dans le navigateur :** http://localhost:9090
- Aller dans **"Graph"** → rechercher les métriques
- Aller dans **"Targets"** → vérifier que tous sont **UP**

### Tester Tempo

```bash
# Vérifier l'état
curl -s http://localhost:3200/status
# Résultat : {"status":"running"}

# Vérifier le ready
curl -s http://localhost:3200/ready
# Résultat : ready
```

**Dans le navigateur :** http://localhost:3200/status

### Tester Grafana

1. Ouvrir http://localhost:3000
2. User: `admin` / Password: `admin`
3. Menu → **"Data Sources"** → Vérifier Prometheus et Tempo
4. Menu → **"Explore"** → Tester les requêtes
5. Menu → **"Dashboards"** → **"Prefect Pipeline - Vue Globale"**

---

## 🔧 Personnalisation du Pipeline

### Ajouter une nouvelle source de données

Modifier `src/tasks/ingestion.py` :
```python
def read_new_format_file(file_path):
    """Lit un nouveau format de fichier"""
    # Implémentation
    return data

# Ajouter dans read_data_from_source()
elif source_type == 'new_format':
    return read_new_format_file(actual_path)
```

### Modifier les règles de validation

Éditer `src/tasks/validation.py` :

```python
@task(name="validation", log_prints=True)
def validation_task(deduplicated_data: dict):
    errors = []
    
    # Règle 1 : Champ obligatoire
    if "user_id" not in deduplicated_data:
        errors.append("Missing required field: user_id")
    
    # Règle 2 : Plage de valeurs
    if "amount" in deduplicated_data:
        if deduplicated_data["amount"] < 0:
            errors.append("Amount must be positive")
        if deduplicated_data["amount"] > 100000:
            errors.append("Amount exceeds maximum limit")
    
    # Règle 3 : Format email
    if "email" in deduplicated_data:
        if "@" not in deduplicated_data["email"]:
            errors.append("Invalid email format")
    
    is_valid = len(errors) == 0
    
    return {
        "status": "valid" if is_valid else "rejected",
        "validated_data": deduplicated_data if is_valid else None,
        "errors": errors,
        "step": "validation"
    }
```

### Ajouter une nouvelle tâche au flow

Éditer `src/flows/normalization_flow.py` :

```python
from ..tasks.new_task import new_task

@flow(name="Data Normalization Pipeline", log_prints=True)
def normalization_flow(...):
    # ... étapes existantes ...
    
    # Nouvelle étape
    print("\n🆕 ÉTAPE X: NOUVELLE TÂCHE")
    result_new = new_task(previous_result)
    
    # ... suite du pipeline ...
```

### Modifier les métriques

Éditer `src/flows/metrics_flow.py` pour ajouter de nouvelles métriques.

---

## 🐛 Dépannage

### Prefect ne démarre pas

```bash
# Vérifier les logs
docker-compose logs prefect-server

# Vérifier que PostgreSQL est healthy
docker-compose ps postgres

# Redémarrer PostgreSQL si nécessaire
docker-compose restart postgres

# Réinitialiser Prefect
docker-compose down -v
docker-compose up -d
```

### MongoDB connexion refusée

```bash
# Vérifier que MongoDB est démarré
docker-compose ps mongodb

# Vérifier les logs
docker-compose logs mongodb

# Vérifier les identifiants dans mongodb_writer.py
```

### Aucune métrique dans l'OTEL Collector

```bash
# 1. Vérifier que metrics_flow.py est bien déployé
docker exec -it prefect-prefect-worker-1 prefect deployment ls

# 2. Exécuter le flow de métriques
curl -X POST "http://localhost:4200/api/deployments/metrics-pipeline/run" \
  -H "Content-Type: application/json" \
  -d '{}'

# 3. Vérifier les métriques
curl -s http://localhost:8890/metrics | grep "prefect"
```

### Fichier non trouvé dans /app/data/

```bash
# Copier les fichiers dans le conteneur
docker cp data/. prefect-prefect-worker-1:/app/data/

# Vérifier
docker exec -it prefect-prefect-worker-1 ls -la /app/data/
```

### Le déploiement ne s'affiche pas dans l'UI

```bash
# Vérifier que le worker est en ligne
docker exec -it prefect-prefect-worker-1 prefect worker status --pool local-pool

# Relancer le déploiement
docker exec -it prefect-prefect-worker-1 python /app/deployments/deploy.py

# Vérifier les déploiements
docker exec -it prefect-prefect-worker-1 prefect deployment ls
```

### Nettoyer les données et recommencer

```bash
# Nettoyer MongoDB
docker exec -it prefect-mongodb-1 mongosh -u admin -p changeme --eval '
use data_pipeline;
db.raw_data.deleteMany({});
db.normalized_data.deleteMany({});
db.rejected_data.deleteMany({});
db.script_metrics.deleteMany({});
print("✅ Nettoyé");
'
```

### Problèmes de ports

```bash
# Voir les ports occupés
netstat -ano | findstr "4200 4317 4318 4319 8890 9090 3000 3200 55680 27017"

# Relancer proprement
docker-compose down && docker-compose up -d
```

---

## 📊 Commandes Utiles

### Gestion des conteneurs

```bash
# Démarrer tous les services
docker-compose up -d

# Arrêter tous les services
docker-compose down

# Arrêter et supprimer les volumes (réinitialisation complète)
docker-compose down -v

# Redémarrer un service
docker-compose restart prefect-server

# Voir les logs d'un service
docker-compose logs -f mongodb

# Voir les logs des 100 dernières lignes
docker-compose logs --tail=100 prefect-server
```

### Prefect CLI

```bash
# Accéder au conteneur worker
docker exec -it prefect-prefect-worker-1 bash

# Lister les déploiements
prefect deployment ls

# Voir les détails d'un déploiement
prefect deployment inspect normalization-pipeline

# Voir les flow runs
prefect flow-run ls

# Voir les logs d'un flow run
prefect flow-run logs <flow-run-id>

# Voir le statut du worker
prefect worker status --pool local-pool
```

### Base de données

```bash
# Connexion à MongoDB
docker exec -it prefect-mongodb-1 mongosh -u admin -p changeme

use data_pipeline
show collections

# Compter les documents
db.raw_data.count()
db.normalized_data.count()
db.rejected_data.count()
db.script_metrics.count()

# Voir les 5 derniers documents
db.script_metrics.find().sort({timestamp: -1}).limit(5).pretty()
```

### Observabilité

```bash
# Voir les métriques OpenTelemetry
curl -s http://localhost:8890/metrics | grep "prefect"

# Voir les métriques Prometheus
curl -s 'http://localhost:9090/api/v1/query?query=prefect_raw_data_total_ratio'

# Vérifier l'état de Tempo
curl -s http://localhost:3200/status
curl -s http://localhost:3200/ready

# Supprimer les métriques Prometheus
curl -X POST -g 'http://localhost:9090/api/v1/admin/tsdb/delete_series?match[]={__name__=~"prefect.*"}'
curl -X POST 'http://localhost:9090/api/v1/admin/tsdb/clean_tombstones'
```

### Nettoyage complet des données

```bash
docker exec -it prefect-mongodb-1 mongosh -u admin -p changeme --eval '
use data_pipeline;
db.raw_data.deleteMany({});
db.normalized_data.deleteMany({});
db.rejected_data.deleteMany({});
db.script_metrics.deleteMany({});
print("✅ Toutes les données supprimées");
'
```

---

## 📦 Dépendances

### Versions des images Docker

| Service | Image | Version |
|---------|-------|---------|
| Prefect | `prefecthq/prefect` | 3-latest |
| PostgreSQL | `postgres` | 14 |
| Redis | `redis` | 7 |
| MongoDB | `mongo` | 7 |
| OpenTelemetry Collector | `otel/opentelemetry-collector-contrib` | latest |
| Prometheus | `prom/prometheus` | latest |
| Tempo | `grafana/tempo` | latest |
| Grafana | `grafana/grafana` | latest |

### Dépendances Python

```txt
# Base de données
pymongo==4.6.1

# Manipulation de données
pandas==2.2.0
numpy==1.26.4
openpyxl==3.1.2
pyarrow==14.0.1

# Lecture de fichiers
xlrd==2.0.1
beautifulsoup4==4.12.2
lxml==5.1.0

# API et requêtes
requests==2.31.0

# SQL
sqlalchemy==2.0.23
psycopg2-binary==2.9.9

# Utilitaires
python-dateutil==2.8.2
pytz==2024.1
```

---

## 📊 Tableau des Ports

| Service | Port Externe | Port Interne | Usage |
|---------|--------------|--------------|-------|
| Prefect UI | 4200 | 4200 | Interface Web Prefect |
| Prefect API | 4200 | 4200 | API Prefect |
| MongoDB | 27017 | 27017 | Base de données |
| Grafana | 3000 | 3000 | Interface Grafana |
| Prometheus | 9090 | 9090 | Interface Prometheus |
| OTEL Collector gRPC | 4317 | 4317 | Réception OTLP gRPC |
| OTEL Collector HTTP | 4318 | 4318 | Réception OTLP HTTP |
| OTEL Collector Metrics | 8890 | 8890 | Exposition métriques |
| Tempo UI | 3200 | 3200 | Interface Tempo |
| Tempo OTLP | 55680 | 55680 | Réception OTLP gRPC |

---

## 🆚 Comparaison des Orchestrateurs

| Fonctionnalité | Windmill | Prefect | Kestra |
|----------------|----------|---------|--------|
| **Interface Web** | ✅ Éditeur intégré | ✅ Monitoring | ✅ Éditeur YAML |
| **Éditeur de code intégré** | ✅ | ❌ | ❌ |
| **Gestion multi-utilisateurs** | ✅ (workspaces) | ✅ (API key) | ✅ |
| **Déploiement des flows** | Intégré | Via CLI/API | Via UI/API |
| **Documentation intégrée** | ✅ | ✅ (docstrings) | ✅ |
| **Logs d'exécution** | ✅ | ✅ | ✅ |
| **Paramètres restreints** | ✅ (enum) | ✅ (Literal) | ✅ (enum) |
| **Orchestration avancée** | ✅ | ✅ (plus mature) | ✅ |
| **Artifacts** | ❌ | ✅ | ✅ |
| **Multi-workers** | ✅ | ✅ | ✅ |
| **Scheduling** | ✅ | ✅ (plus avancé) | ✅ |
| **Observabilité** | ✅ (OTEL) | ✅ (OTEL) | ✅ (OTEL) |
| **Langages supportés** | Python, TypeScript | Python | Python, Node.js, Shell |

---

## 📚 Documentation Liée

- [Prefect Documentation](https://docs.prefect.io/)
- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [MongoDB Documentation](https://docs.mongodb.com/)
- [Prometheus Documentation](https://prometheus.io/docs/)
- [Tempo Documentation](https://grafana.com/docs/tempo/latest/)

---

## 📄 Licence

MIT

---

## 🔄 Historique des Versions

| Version | Date | Description |
|---------|------|-------------|
| v1.0.0 | 2026-07-01 | Version finale Prefect |
| | | - Pipeline 12 étapes complet |
| | | - Support CSV, JSON, XML, Excel, Parquet, HTML, API, GraphQL, SQL |
| | | - MongoDB multi-collections (raw/normalized/rejected) |
| | | - Stack OTEL + Prometheus + Tempo + Grafana |
| | | - Dashboard Grafana complet avec toutes les métriques |
| | | - Données de test intégrées dans dossier `data/` |
| | | - Métriques de latence, CPU, Mémoire, Erreurs |
| | | - Interface Prefect pour le monitoring |
| | | - Déploiements via CLI |
| | | - Artifacts pour la traçabilité |

---

## 💡 Bonnes Pratiques

1. **Sécurité** : Changez les mots de passe par défaut dans `.env` et `docker-compose.yml`
2. **Performances** : Ajustez les ressources CPU/Mémoire dans `docker-compose.yml`
3. **Logs** : Configurez la rotation des logs via les variables d'environnement
4. **Backup** : Sauvegardez régulièrement les volumes Docker (postgres_data, mongodb_data)
5. **Monitoring** : Utilisez les dashboards Grafana pour surveiller la santé du pipeline
6. **Tests** : Après chaque modification, exécutez le pipeline et vérifiez les métriques
7. **Versioning** : Utilisez les tags de version pour les déploiements Prefect
8. **Workers** : Ajustez le nombre de workers selon la charge de travail

---

**Prêt à utiliser !** 🚀 Lancez `docker-compose up -d` et commencez à orchestrer vos données avec Prefect.